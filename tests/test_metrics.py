"""Prometheus metrics: each code path counts what it did, with low-cardinality labels only."""

import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.adapters.kafka.relay import OutboxCollector, OutboxRelay
from feed_fanout.adapters.kafka.worker import Outcome
from feed_fanout.adapters.postgres.models import OutboxEvent
from feed_fanout.health import Health, HealthServer
from tests.conftest import Worker
from tests.helpers import create_post, follow
from tests.test_fanout import DB_DOWN, failing_n_times, produce_raw


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def delta(name: str, **labels: str) -> Callable[[], float]:
    """Returns a function giving how much the sample changed since delta() was called."""
    before = sample(name, **labels)
    return lambda: sample(name, **labels) - before


# --- API --------------------------------------------------------------------------


def test_requests_are_labelled_by_route_template_not_raw_path(client: TestClient) -> None:
    feed_reads = delta("feed_fanout_http_requests_total", method="GET", route="/users/{user_id}/feed", status="200")

    client.get("/users/bob/feed")
    client.get("/users/carol/feed")

    assert feed_reads() == 2  # one series for all users...
    exposition = client.get("/metrics").text
    assert "/users/bob/feed" not in exposition  # ...never one per user


def test_request_latency_is_recorded(client: TestClient) -> None:
    timed = delta("feed_fanout_http_request_duration_seconds_count", method="POST", route="/posts")

    create_post(client, "alice")

    assert timed() == 1


def test_unknown_paths_share_one_label(client: TestClient) -> None:
    unmatched = delta("feed_fanout_http_requests_total", method="GET", route="unmatched", status="404")

    client.get("/no/such/path")

    assert unmatched() == 1


def test_metrics_endpoint(client: TestClient) -> None:
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "feed_fanout_http_requests_total" in response.text


# --- Worker -----------------------------------------------------------------------


def test_fanout_is_counted_timed_and_sized(client: TestClient, worker: Worker) -> None:
    fanned = delta("feed_fanout_worker_events_total", outcome="fanned_out")
    fanouts = delta("feed_fanout_worker_fanout_duration_seconds_count")
    rows = delta("feed_fanout_worker_feed_rows_written_sum")
    post_to_feed = delta("feed_fanout_post_to_feed_seconds_count")
    follow(client, "bob", "alice")
    follow(client, "carol", "alice")

    create_post(client, "alice")
    worker.process_next()

    assert fanned() == 1
    assert fanouts() == 1
    assert rows() == 2  # two followers
    assert post_to_feed() == 1


def test_dead_letters_are_counted_by_reason(worker: Worker, kafka_bootstrap: str, topic: str) -> None:
    malformed = delta("feed_fanout_dead_letters_total", reason="malformed")
    dead_lettered = delta("feed_fanout_worker_events_total", outcome="dead_lettered")
    produce_raw(kafka_bootstrap, topic, key="alice", value=b"garbage")

    assert worker.process_next() == Outcome.DEAD_LETTERED

    assert malformed() == 1
    assert dead_lettered() == 1


def test_retries_are_counted_by_kind(client: TestClient, worker: Worker, monkeypatch: pytest.MonkeyPatch) -> None:
    transient = delta("feed_fanout_worker_retries_total", kind="transient")
    follow(client, "bob", "alice")
    create_post(client, "alice")
    failing_n_times(2, DB_DOWN, monkeypatch)

    worker.process_next()

    assert transient() == 2


def test_consumer_lag(worker: Worker, kafka_bootstrap: str, topic: str) -> None:
    # Three events on one partition (same key); the worker handles the first.
    for _ in range(3):
        produce_raw(kafka_bootstrap, topic, key="alice", value=b"garbage")
    worker.process_next()

    worker.fanout.update_lag(every=0)

    lag = sum(s.value for m in REGISTRY.collect() if m.name == "feed_fanout_worker_consumer_lag" for s in m.samples)
    assert lag == 2  # newest offset 3, our position 1: two events still to handle


# --- Relay ------------------------------------------------------------------------


def test_relay_counts_published_and_failed(
    client: TestClient, relay: OutboxRelay, session_factory: sessionmaker[Session]
) -> None:
    published = delta("feed_fanout_relay_published_total")
    failed = delta("feed_fanout_relay_publish_failures_total")
    create_post(client, "alice")
    create_post(client, "bob")
    with session_factory.begin() as session:
        session.add(OutboxEvent(topic="no-such-topic", key="k", payload="{}"))

    relay.publish_batch()

    assert published() == 2
    assert failed() == 1


def outbox_gauges(collector: OutboxCollector) -> dict[str, float]:
    return {m.name: m.samples[0].value for m in collector.collect()}


def test_outbox_backlog_and_oldest_age(session_factory: sessionmaker[Session]) -> None:
    collector = OutboxCollector(session_factory)
    assert outbox_gauges(collector) == {"feed_fanout_outbox_backlog": 0, "feed_fanout_outbox_oldest_age_seconds": 0}

    with session_factory.begin() as session:
        old = datetime.now(UTC) - timedelta(minutes=5)
        session.add(OutboxEvent(topic="t", key="k", payload="{}", created_at=old))
        session.add(OutboxEvent(topic="t", key="k", payload="{}"))

    gauges = outbox_gauges(collector)
    assert gauges["feed_fanout_outbox_backlog"] == 2
    assert 299 < gauges["feed_fanout_outbox_oldest_age_seconds"] < 330  # the oldest one: ~5 minutes


def test_outbox_metrics_skip_when_database_is_down() -> None:
    unreachable = sessionmaker(create_engine("postgresql+psycopg://nobody:nothing@localhost:1/none"))

    assert outbox_gauges(OutboxCollector(unreachable)) == {}  # no gauges, but no failed scrape either


# --- Health server ----------------------------------------------------------------


class Healthy:
    def health(self) -> Health:
        return Health(live=True, ready=True)


def test_health_server_serves_metrics() -> None:
    server = HealthServer(Healthy(), port=0).start()
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{server.port}/metrics", timeout=5) as response:
            body = response.read().decode()
        assert response.status == 200
        assert "feed_fanout_worker_events_total" in body
    finally:
        server.stop()
