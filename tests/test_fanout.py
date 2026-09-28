"""The worker: posts reach followers' feeds, safely under re-delivery and failures."""

import uuid
from datetime import UTC, datetime

import pytest
from confluent_kafka import Producer, TopicPartition
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

import feed_fanout.worker
from feed_fanout.events import PostCreated
from feed_fanout.models import FeedItem
from feed_fanout.worker import Outcome, PermanentError, handle
from tests.conftest import TopicReader, Worker
from tests.helpers import create_post, feed_ids, follow


def feed_row_count(session_factory: sessionmaker[Session]) -> int:
    with session_factory() as session:
        return session.scalar(select(func.count()).select_from(FeedItem)) or 0


def produce_raw(bootstrap: str, topic: str, key: str, value: bytes) -> None:
    producer = Producer({"bootstrap.servers": bootstrap, "partitioner": "murmur2_random"})
    producer.produce(topic, key=key, value=value)
    producer.flush(10)
    producer.close()


def headers(msg) -> dict[str, str]:
    return {k: v.decode() for k, v in msg.headers() or []}


# --- Happy path --------------------------------------------------------------

def test_followers_see_post_after_fanout(client: TestClient, worker: Worker) -> None:
    # Given bob and carol follow alice
    follow(client, "bob", "alice")
    follow(client, "carol", "alice")

    # When alice posts and the worker processes the event
    post = create_post(client, "alice")
    assert worker.process_next() == Outcome.FANNED_OUT

    # Then both followers have the post in their feed
    assert feed_ids(client, "bob") == [post["id"]]
    assert feed_ids(client, "carol") == [post["id"]]


def test_non_followers_do_not_see_post(client: TestClient, worker: Worker) -> None:
    follow(client, "bob", "alice")
    follow(client, "alice", "dave")  # the reverse direction: alice follows dave

    create_post(client, "alice")
    worker.process_next()

    assert feed_ids(client, "dave") == []
    assert feed_ids(client, "alice") == []  # authors don't get their own posts in their feed


def test_post_before_follow_is_not_backfilled(client: TestClient, worker: Worker) -> None:
    # Fanout-on-write: a post only reaches the followers that exist when it's fanned out.
    create_post(client, "alice")
    worker.process_next()

    follow(client, "bob", "alice")

    assert feed_ids(client, "bob") == []


def test_reprocessing_same_event_adds_no_duplicates(
    client: TestClient, worker: Worker, session_factory: sessionmaker[Session]
) -> None:
    # Given a post that has already been fanned out
    follow(client, "bob", "alice")
    follow(client, "carol", "alice")
    post = create_post(client, "alice")
    worker.process_next()
    assert feed_row_count(session_factory) == 2

    # When the same event is delivered again (at-least-once)
    event = PostCreated(post_id=post["id"], author_id="alice", created_at=post["created_at"])
    added = handle(event.model_dump_json().encode(), session_factory)

    # Then nothing changes
    assert added == 0
    assert feed_row_count(session_factory) == 2
    assert feed_ids(client, "bob") == [post["id"]]


# --- Permanent failures: dead-letter topic -----------------------------------

@pytest.mark.parametrize("value", [b"not json", b'{"post_id": "p1", "author": "alice"}', b"", None])
def test_malformed_payload_is_a_permanent_error(value: bytes | None, session_factory: sessionmaker[Session]) -> None:
    with pytest.raises(PermanentError) as e:
        handle(value, session_factory)
    assert e.value.reason == "malformed"


def test_malformed_event_is_dead_lettered_with_context(
    worker: Worker, read_dead_letters: TopicReader, kafka_bootstrap: str, topic: str
) -> None:
    produce_raw(kafka_bootstrap, topic, key="alice", value=b"garbage")

    assert worker.process_next() == Outcome.DEAD_LETTERED

    [dead] = read_dead_letters(1)
    # The original event is preserved exactly...
    assert dead.key() == b"alice"
    assert dead.value() == b"garbage"
    # ...with the reason and its origin in headers.
    h = headers(dead)
    assert h["dlt.reason"] == "malformed"
    assert h["dlt.original.topic"] == topic
    assert h["dlt.original.offset"] == "0"
    assert "dlt.error" in h and "dlt.failed_at" in h


def test_offset_is_committed_after_dead_lettering(
    worker: Worker, read_dead_letters: TopicReader, kafka_bootstrap: str, topic: str
) -> None:
    # A dead-lettered event is done: it must not be re-delivered to the main topic's consumers.
    produce_raw(kafka_bootstrap, topic, key="alice", value=b"garbage")
    worker.process_next()
    read_dead_letters(1)

    committed = worker.fanout.consumer.committed(
        [TopicPartition(topic, p) for p in range(3)], timeout=10
    )
    assert sorted(tp.offset for tp in committed if tp.offset >= 0) == [1]


def test_event_for_unknown_post_is_dead_lettered(
    client: TestClient, worker: Worker, read_dead_letters: TopicReader,
    session_factory: sessionmaker[Session], kafka_bootstrap: str, topic: str,
) -> None:
    follow(client, "bob", "alice")
    event = PostCreated(post_id=uuid.uuid4(), author_id="alice", created_at=datetime.now(UTC))
    produce_raw(kafka_bootstrap, topic, key="alice", value=event.model_dump_json().encode())

    assert worker.process_next() == Outcome.DEAD_LETTERED

    [dead] = read_dead_letters(1)
    assert headers(dead)["dlt.reason"] == "unknown_post"
    assert feed_row_count(session_factory) == 0


def test_worker_keeps_going_after_a_dead_lettered_event(
    client: TestClient, worker: Worker, read_dead_letters: TopicReader, kafka_bootstrap: str, topic: str
) -> None:
    # Given a malformed message followed by a valid post, both from alice (same partition, in order)
    produce_raw(kafka_bootstrap, topic, key="alice", value=b"garbage")
    follow(client, "bob", "alice")
    post = create_post(client, "alice")

    # When the worker processes both
    assert worker.process_next() == Outcome.DEAD_LETTERED
    assert worker.process_next() == Outcome.FANNED_OUT

    # Then the valid post still got through
    assert feed_ids(client, "bob") == [post["id"]]
    read_dead_letters(1)


# --- Transient and unexpected failures: retry --------------------------------

def failing_n_times(n: int, error: Exception, monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Make fan_out raise `error` for its first n calls, then behave normally."""
    calls: list[int] = []
    real = feed_fanout.worker.fan_out

    def flaky(*args, **kwargs):
        calls.append(1)
        if len(calls) <= n:
            raise error
        return real(*args, **kwargs)

    monkeypatch.setattr(feed_fanout.worker, "fan_out", flaky)
    return calls


DB_DOWN = OperationalError("INSERT ...", {}, Exception("connection refused"))


def test_database_outage_is_retried_until_it_succeeds(
    client: TestClient, worker: Worker, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Transient failures are retried indefinitely, far past the unexpected-error limit (3).
    follow(client, "bob", "alice")
    post = create_post(client, "alice")
    calls = failing_n_times(5, DB_DOWN, monkeypatch)

    assert worker.process_next() == Outcome.FANNED_OUT

    assert len(calls) == 6
    assert feed_ids(client, "bob") == [post["id"]]


def test_unexpected_error_is_retried_then_dead_lettered(
    client: TestClient, worker: Worker, read_dead_letters: TopicReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    follow(client, "bob", "alice")
    create_post(client, "alice")
    calls = failing_n_times(100, ValueError("bug"), monkeypatch)

    assert worker.process_next() == Outcome.DEAD_LETTERED

    assert len(calls) == 3
    [dead] = read_dead_letters(1)
    assert headers(dead)["dlt.reason"] == "unexpected"
    assert "bug" in headers(dead)["dlt.error"]


def test_unexpected_error_that_recovers_is_not_dead_lettered(
    client: TestClient, worker: Worker, monkeypatch: pytest.MonkeyPatch
) -> None:
    follow(client, "bob", "alice")
    post = create_post(client, "alice")
    failing_n_times(2, ValueError("flaky"), monkeypatch)

    assert worker.process_next() == Outcome.FANNED_OUT
    assert feed_ids(client, "bob") == [post["id"]]
