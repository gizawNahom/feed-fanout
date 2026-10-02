"""Structured logs: one JSON object per line, with fields you can filter on."""

import io
import json
import logging
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from feed_fanout.logs import configure_logging, get_logger
from tests.conftest import Worker
from tests.helpers import create_post, follow
from tests.test_fanout import produce_raw


@pytest.fixture
def json_logs() -> Iterator[Any]:
    """Configure JSON logging into a buffer; returns a reader of the parsed lines.

    (A buffer rather than pytest's capsys: the handler would keep the sys.stdout that pytest
    swaps out between fixture setup and the test body.)
    """
    root = logging.getLogger()
    saved = root.handlers[:], root.level
    configure_logging("INFO", "json")
    buffer = io.StringIO()
    handler = root.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    handler.setStream(buffer)

    def read() -> list[dict[str, Any]]:
        return [json.loads(line) for line in buffer.getvalue().splitlines()]

    yield read
    root.handlers, level = saved
    root.setLevel(level)


def test_log_line_is_json_with_fields(json_logs: Any) -> None:
    get_logger("feed_fanout.test").info("fanned out", feeds_added=3, post_id="p1")

    [line] = json_logs()
    assert line["msg"] == "fanned out"
    assert line["level"] == "info"
    assert line["logger"] == "feed_fanout.test"
    assert line["feeds_added"] == 3 and line["post_id"] == "p1"
    assert line["ts"].endswith("Z")


def test_third_party_logs_use_the_same_format(json_logs: Any) -> None:
    logging.getLogger("uvicorn.access").info("GET /health/live 200")

    [line] = json_logs()
    assert line["msg"] == "GET /health/live 200"
    assert line["logger"] == "uvicorn.access"


def test_exception_is_one_structured_field(json_logs: Any) -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        get_logger("feed_fanout.test").exception("giving up")

    [line] = json_logs()  # one line, not a multi-line traceback
    assert line["exception"][0]["exc_type"] == "ValueError"
    assert line["exception"][0]["exc_value"] == "boom"


def test_every_line_for_an_event_carries_its_ids_and_they_do_not_leak(
    json_logs: Any, client: TestClient, worker: Worker, kafka_bootstrap: str, topic: str
) -> None:
    # Given a valid post, then a malformed event on the same partition
    follow(client, "bob", "alice")
    post = create_post(client, "alice")
    worker.process_next()
    produce_raw(kafka_bootstrap, topic, key="alice", value=b"garbage")
    worker.process_next()

    lines = json_logs()
    fanned = next(line for line in lines if line["msg"] == "fanned out")
    dead = next(line for line in lines if line["msg"] == "dead-lettered")

    # The fanout line carries the post's ids and position, without passing them around by hand...
    assert fanned["post_id"] == post["id"]
    assert fanned["author_id"] == "alice"
    assert {"partition", "offset"} <= fanned.keys()
    # ...and the next event's lines don't inherit them.
    assert "post_id" not in dead
    assert dead["offset"] == fanned["offset"] + 1


@pytest.mark.parametrize(
    ("uvicorn_state", "logged"),
    [
        ({"handlers": [logging.NullHandler()], "propagate": False}, True),  # uvicorn's default: access log on
        ({"handlers": [], "propagate": False}, False),  # what --no-access-log leaves behind
    ],
    ids=["access-log", "no-access-log"],
)
def test_uvicorn_access_log_follows_uvicorns_setting(uvicorn_state: dict[str, Any], logged: bool) -> None:
    access, root = logging.getLogger("uvicorn.access"), logging.getLogger()
    saved = access.handlers[:], access.propagate, root.handlers[:], root.level
    access.handlers, access.propagate = uvicorn_state["handlers"], uvicorn_state["propagate"]
    try:
        configure_logging("INFO", "json")
        buffer = io.StringIO()
        handler = root.handlers[0]
        assert isinstance(handler, logging.StreamHandler)
        handler.setStream(buffer)

        access.info('127.0.0.1:1234 - "GET /me/feed HTTP/1.1" 200')

        assert bool(buffer.getvalue()) is logged
    finally:
        access.handlers, access.propagate, root.handlers, level = saved
        root.setLevel(level)
