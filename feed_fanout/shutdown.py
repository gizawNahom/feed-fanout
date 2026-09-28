"""Graceful shutdown on SIGTERM/SIGINT, shared by the worker and the relay."""

import signal
from collections.abc import Callable
from types import FrameType

from feed_fanout.logs import get_logger

log = get_logger(__name__)


def install_signal_handlers(stop: Callable[[], None]) -> None:
    """On SIGTERM (docker stop, Kubernetes, deploys) or SIGINT (Ctrl+C), call stop().

    stop() only sets a flag: the process finishes what it's doing, cleans up, and exits.
    A second signal falls back to the default behavior and exits immediately.
    """

    def handler(signum: int, _frame: FrameType | None) -> None:
        log.info("shutting down: finishing current work (repeat signal to force)", signal=signal.Signals(signum).name)
        stop()
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.default_int_handler)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
