"""Health checks: liveness means "not stuck", never "dependencies are up"."""

import json
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.adapters.http import app, get_session, get_token_verifier
from feed_fanout.adapters.kafka.producers import DeadLetterPublisher
from feed_fanout.adapters.kafka.relay import OutboxRelay
from feed_fanout.adapters.kafka.worker import FanoutWorker, make_consumer
from feed_fanout.health import Health, HealthServer, Heartbeat, start_health_server
from tests.conftest import FAST_RETRY, TEST_VERIFIER, postgres_fan_out


def get(port: int, path: str) -> tuple[int, dict[str, Any]]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


# --- API ------------------------------------------------------------------------


def test_api_live_and_ready(client: TestClient) -> None:
    assert client.get("/health/live").status_code == 200

    response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "checks": {"database": "ok"}}


def test_api_not_ready_but_still_live_when_database_is_down() -> None:
    # A database the API can't reach (nothing listens on port 1)
    unreachable = sessionmaker(create_engine("postgresql+psycopg://nobody:nothing@localhost:1/none"))

    def broken_session() -> Iterator[Session]:
        with unreachable() as session:
            yield session

    app.dependency_overrides[get_session] = broken_session
    app.dependency_overrides[get_token_verifier] = lambda: TEST_VERIFIER
    try:
        with TestClient(app) as client:
            ready = client.get("/health/ready")
            live = client.get("/health/live")
    finally:
        app.dependency_overrides.clear()

    # Not ready: stop sending it traffic...
    assert ready.status_code == 503
    assert ready.json()["status"] == "unavailable"
    assert "database" in ready.json()["checks"]
    # ...but alive: restarting it wouldn't fix the database.
    assert live.status_code == 200


# --- Heartbeat --------------------------------------------------------------------


def test_heartbeat_goes_stale_when_the_loop_stops() -> None:
    clock = FakeClock()
    beat = Heartbeat(stale_after=60, clock=clock)
    assert beat.live()

    clock.now += 59
    assert beat.live()
    clock.now += 2  # 61s without a beat: stuck
    assert not beat.live()

    beat.beat()
    assert beat.live()


def test_heartbeat_tracks_failures_and_success() -> None:
    clock = FakeClock()
    beat = Heartbeat(clock=clock)
    beat.failure()
    beat.failure()
    assert beat.details()["consecutive_failures"] == 2
    assert beat.details()["last_success_seconds_ago"] is None

    clock.now += 5
    beat.success()
    clock.now += 3
    assert beat.details()["consecutive_failures"] == 0
    assert beat.details()["last_success_seconds_ago"] == 3


# --- Health server ----------------------------------------------------------------


class Fixed:
    def __init__(self, health: Health) -> None:
        self.value = health

    def health(self) -> Health:
        return self.value


@pytest.fixture
def server() -> Iterator[tuple[HealthServer, Fixed]]:
    check = Fixed(Health(live=True, ready=True, details={"state": "consuming"}))
    s = HealthServer(check, port=0).start()  # port 0: any free port
    yield s, check
    s.stop()


def test_server_reports_live_and_ready(server: tuple[HealthServer, Fixed]) -> None:
    s, _ = server
    assert get(s.port, "/health/live") == (200, {"status": "ok", "live": True, "ready": True, "state": "consuming"})
    assert get(s.port, "/health/ready")[0] == 200


def test_server_returns_503_for_failing_probe_only(server: tuple[HealthServer, Fixed]) -> None:
    s, check = server
    check.value = Health(live=True, ready=False)

    assert get(s.port, "/health/live")[0] == 200
    status, body = get(s.port, "/health/ready")
    assert status == 503 and body["status"] == "unavailable"


def test_server_unknown_path(server: tuple[HealthServer, Fixed]) -> None:
    assert get(server[0].port, "/nope")[0] == 404


def test_port_in_use_disables_health_instead_of_crashing(server: tuple[HealthServer, Fixed]) -> None:
    # e.g. two workers on one machine with the same port: the second keeps working.
    assert start_health_server(Fixed(Health(True, True)), server[0].port) is None


def test_port_zero_disables_health() -> None:
    assert start_health_server(Fixed(Health(True, True)), 0) is None


# --- Worker and relay ---------------------------------------------------------------


def test_worker_ready_while_consuming_and_not_after_stop(
    kafka_bootstrap: str, topic: str, dlt_topic: str, session_factory: sessionmaker[Session]
) -> None:
    worker = FanoutWorker(
        make_consumer(kafka_bootstrap, "test-health"),
        postgres_fan_out(session_factory),
        DeadLetterPublisher(kafka_bootstrap, dlt_topic),
        FAST_RETRY,
    )
    assert not worker.health().ready  # not consuming yet

    thread = threading.Thread(target=worker.run, args=(topic,))
    thread.start()
    try:
        deadline = time.monotonic() + 20
        while not worker.health().ready and time.monotonic() < deadline:
            time.sleep(0.1)
        health = worker.health()
        assert health.live and health.ready
        assert health.details["state"] == "consuming"

        worker.stop()
        # Shutting down: no longer ready (an orchestrator knows it's on its way out).
        assert not worker.health().ready
    finally:
        worker.stop()
        thread.join(timeout=10)


def test_worker_stays_live_while_backing_off(worker: Any) -> None:
    # A worker waiting out a database outage is doing the right thing: it must not look stuck,
    # or the orchestrator would restart it (which can't fix the outage).
    fanout: FanoutWorker = worker.fanout
    clock = FakeClock()
    fanout.heartbeat = Heartbeat(stale_after=60, clock=clock)
    clock.now += 120  # the last beat looks ancient
    assert not fanout.health().live

    fanout._wait(0.05)  # the backoff wait

    assert fanout.health().live
    assert fanout.health().details["state"] == "backing_off"


def test_relay_health_reflects_publish_failures(relay: OutboxRelay, session_factory: sessionmaker[Session]) -> None:
    from feed_fanout.adapters.postgres.models import OutboxEvent

    with session_factory.begin() as session:
        session.add(OutboxEvent(topic="no-such-topic", key="k", payload="{}"))  # will fail to publish
    thread = threading.Thread(target=relay.run, kwargs={"retry": FAST_RETRY})
    thread.start()
    try:
        deadline = time.monotonic() + 30
        while relay.health().details["consecutive_failures"] < 1 and time.monotonic() < deadline:
            time.sleep(0.1)
        health = relay.health()
        assert health.live and health.ready  # failing to publish isn't being stuck
        assert health.details["consecutive_failures"] >= 1
    finally:
        relay.stop()
        thread.join(timeout=15)
    assert not relay.health().ready
