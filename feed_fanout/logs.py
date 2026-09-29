"""
Structured logging for every process (API, worker, relay, replay tool).

Log calls pass fields as keywords, not formatted into the message:

    log.info("fanned out", post_id=event.post_id, feeds_added=3, partition=0, offset=42)

and each line becomes either JSON (for log systems: filter on post_id, not grep text)

    {"ts": "2026-09-28T10:00:00Z", "level": "info", "logger": "feed_fanout.adapters.kafka.worker",
     "msg": "fanned out", "post_id": "…", "feeds_added": 3, "partition": 0, "offset": 42}

or readable text for a terminal. LOG_FORMAT=auto (the default) picks text when stdout is a
terminal and JSON otherwise (containers, CI, files).

Third-party logs (uvicorn, alembic, ...) go through the same pipeline, so every line of a
process has one format.
"""

import logging
import sys
from typing import Literal

import structlog
from structlog.typing import Processor

LogFormat = Literal["auto", "json", "text"]


def configure_logging(level: str = "INFO", fmt: LogFormat = "auto") -> None:
    use_json = fmt == "json" or (fmt == "auto" and not sys.stdout.isatty())

    # Applied to every record: ours (structlog) and third-party (stdlib logging).
    shared: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.ExtraAdder(),
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
    ]
    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    renderer: list[Processor] = (
        [
            # Tracebacks as one structured field, not many lines that log collectors split apart.
            structlog.processors.dict_tracebacks,
            structlog.processors.EventRenamer("msg"),
            structlog.processors.JSONRenderer(),
        ]
        if use_json
        else [structlog.dev.ConsoleRenderer()]
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, *renderer],
    )

    handler = logging.StreamHandler(sys.stdout)  # containers collect stdout
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)

    # uvicorn installs its own handlers; route its logs (incl. access logs) through ours.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).handlers = []
        logging.getLogger(name).propagate = True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return logger
