"""Retry timing and error classification, shared by the worker and the outbox relay."""

import random
from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError


@dataclass(frozen=True)
class RetryPolicy:
    base_delay: float = 1.0          # backoff ceiling for the first retry, doubled each attempt...
    max_delay: float = 30.0          # ...up to this cap
    max_unexpected_attempts: int = 3  # unexpected errors: give up and dead-letter after this many

    def delay(self, attempt: int, rand: Callable[[], float] = random.random) -> float:
        """Exponential backoff with full jitter: a random wait in [0, ceiling).

        Without jitter, workers that failed together (e.g. the database went down for all
        of them) retry together, hitting the recovering database in synchronized waves
        (a "thundering herd"). Randomizing spreads the retries out.
        """
        ceiling = min(self.base_delay * 2 ** (attempt - 1), self.max_delay)
        return rand() * ceiling


def is_transient(e: Exception) -> bool:
    """Database unavailable, connection dropped, deadlock: worth retrying as-is."""
    return isinstance(e, OperationalError | InterfaceError) or (
        isinstance(e, DBAPIError) and e.connection_invalidated
    )
