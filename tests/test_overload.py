"""
Overload protection (ShedLoad): past the limit, new requests get an immediate 503 instead of
queuing until the process runs out of resources (what the first load test showed).

Driven at the ASGI level with an app that holds requests open, so "in flight" is exact.
"""

import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from starlette.types import Receive, Scope, Send

from feed_fanout import metrics
from feed_fanout.adapters.http import RecordMetrics, ShedLoad, app
from feed_fanout.config import settings

Response = tuple[int, dict[str, str]]


class HeldApp:
    """An ASGI app that answers 200, but only once `release` is set."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.entered = 0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        self.entered += 1
        await self.release.wait()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})


async def request(asgi: Any, path: str = "/me/feed") -> Response:
    scope = {"type": "http", "method": "GET", "path": path, "headers": [], "query_string": b""}
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await asgi(scope, receive, send)
    start = next(m for m in sent if m["type"] == "http.response.start")
    return start["status"], {k.decode().lower(): v.decode() for k, v in start["headers"]}


async def until(condition: Callable[[], bool]) -> None:
    for _ in range(100):
        if condition():
            return
        await asyncio.sleep(0.001)
    raise AssertionError("condition never became true")


def test_past_the_limit_requests_are_turned_away_at_once() -> None:
    async def scenario() -> None:
        held = HeldApp()
        shed = ShedLoad(held, limit=2)
        first, second = asyncio.create_task(request(shed)), asyncio.create_task(request(shed))
        await until(lambda: held.entered == 2)

        status, headers = await asyncio.wait_for(request(shed), timeout=1)  # answered without waiting

        assert status == 503
        assert headers["retry-after"] == "1"
        held.release.set()
        assert [await first, await second] == [(200, {}), (200, {})]
        assert shed.in_flight == 0

    asyncio.run(scenario())


def test_probes_and_metrics_are_never_shed() -> None:
    async def scenario() -> None:
        held = HeldApp()
        held.release.set()  # exempt requests reach the app, which answers at once
        shed = ShedLoad(held, limit=1)
        shed.in_flight = 1  # as if at the limit

        for path in ("/health/live", "/health/ready", "/metrics"):
            assert (await request(shed, path))[0] == 200
        assert (await request(shed, "/me/feed"))[0] == 503

    asyncio.run(scenario())


def test_a_failing_request_frees_its_slot() -> None:
    async def failing(scope: Any, receive: Any, send: Any) -> None:
        raise RuntimeError("boom")

    async def scenario() -> None:
        shed = ShedLoad(failing, limit=1)
        with pytest.raises(RuntimeError):
            await request(shed)
        assert shed.in_flight == 0

    asyncio.run(scenario())


def test_shed_requests_are_counted_as_overloaded() -> None:
    def overloaded_503s() -> float:
        value: float = metrics.HTTP_REQUESTS.labels("GET", "overloaded", "503")._value.get()
        return value

    async def scenario() -> None:
        shed = ShedLoad(HeldApp(), limit=1)
        shed.in_flight = 1
        before = overloaded_503s()

        assert (await request(RecordMetrics(shed)))[0] == 503
        assert overloaded_503s() == before + 1

    asyncio.run(scenario())


def test_the_api_sheds_load_at_the_configured_limit() -> None:
    # Starlette types `cls` as a factory protocol, so mypy rejects comparing it with a class.
    [shed] = [m for m in app.user_middleware if m.cls is ShedLoad]  # type: ignore[comparison-overlap]

    assert shed.kwargs == {"limit": settings.api_max_in_flight}
