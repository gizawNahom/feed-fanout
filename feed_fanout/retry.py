"""Retry timing and error classification, shared by the worker and the outbox relay."""

import random
from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError


@dataclass(frozen=True)
class RetryPolicy:
    base_delay: float = 1.0  # backoff ceiling for the first retry, doubled each attempt...
    max_delay: float = 30.0  # ...up to this cap
    max_unexpected_attempts: int = 3  # unexpected errors: give up and dead-letter after this many

    def delay(self, attempt: int, rand: Callable[[], float] = random.random) -> float:
        """Exponential backoff with full jitter: a random wait in [0, ceiling).

        Without jitter, workers that failed together (e.g. the database went down for all
        of them) retry together, hitting the recovering database in synchronized waves
        (a "thundering herd"). Randomizing spreads the retries out.
        """
        ceiling = min(self.base_delay * 2.0 ** (attempt - 1), self.max_delay)
        return rand() * ceiling


DEFAULT_RETRY = RetryPolicy()


# Postgres SQLSTATEs worth retrying as-is, besides the whole "08" class (connection exception).
# https://www.postgresql.org/docs/current/errcodes-appendix.html
TRANSIENT_SQLSTATES = {
    "40001",  # serialization_failure: a concurrent transaction won; running again succeeds
    "40P01",  # deadlock_detected: same
    "57P01",  # admin_shutdown: server is shutting down or restarting
    "57P02",  # crash_shutdown
    "57P03",  # cannot_connect_now: server is starting up
}


def is_transient(e: Exception) -> bool:
    """True if the same operation can succeed later without anything about the event changing.

    Deliberately narrow: transient errors are retried *forever*, so a misclassified error
    that's really about this one event (say, a statement timeout on a huge fanout) would
    block its partition for good. Anything not listed here counts as unexpected: retried a
    few times, then dead-lettered.
    """
    if not isinstance(e, DBAPIError):
        return False
    if e.connection_invalidated:
        return True
    sqlstate = getattr(e.orig, "sqlstate", None)
    if sqlstate is None:
        # No response from the server: couldn't connect, or the connection dropped.
        return isinstance(e, OperationalError | InterfaceError)
    return sqlstate.startswith("08") or sqlstate in TRANSIENT_SQLSTATES
