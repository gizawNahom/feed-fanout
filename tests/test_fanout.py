"""The worker: posts reach followers' feeds, safely under re-delivery and bad input."""

import uuid
from datetime import UTC, datetime

from confluent_kafka import Producer
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.events import PostCreated
from feed_fanout.models import FeedItem
from feed_fanout.worker import Outcome, handle
from tests.conftest import Worker
from tests.helpers import create_post, feed_ids, follow


def feed_row_count(session_factory: sessionmaker[Session]) -> int:
    with session_factory() as session:
        return session.scalar(select(func.count()).select_from(FeedItem)) or 0


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
    outcome = handle(event.model_dump_json().encode(), session_factory)

    # Then nothing changes
    assert outcome == Outcome.FANNED_OUT
    assert feed_row_count(session_factory) == 2
    assert feed_ids(client, "bob") == [post["id"]]


def test_malformed_events_are_skipped(session_factory: sessionmaker[Session]) -> None:
    for value in [b"not json", b'{"post_id": "p1", "author": "alice"}', b"", None]:
        assert handle(value, session_factory) == Outcome.SKIPPED_MALFORMED


def test_event_for_unknown_post_is_skipped(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    follow(client, "bob", "alice")
    event = PostCreated(post_id=uuid.uuid4(), author_id="alice", created_at=datetime.now(UTC))

    assert handle(event.model_dump_json().encode(), session_factory) == Outcome.SKIPPED_UNKNOWN_POST
    assert feed_row_count(session_factory) == 0


def test_worker_keeps_going_after_a_malformed_event(
    client: TestClient, worker: Worker, kafka_bootstrap: str, topic: str
) -> None:
    # Given a malformed message followed by a valid post, both from alice (same partition, in order)
    raw = Producer({"bootstrap.servers": kafka_bootstrap, "partitioner": "murmur2_random"})
    raw.produce(topic, key="alice", value=b"garbage")
    raw.flush(10)
    raw.close()
    follow(client, "bob", "alice")
    post = create_post(client, "alice")

    # When the worker processes both
    assert worker.process_next() == Outcome.SKIPPED_MALFORMED
    assert worker.process_next() == Outcome.FANNED_OUT

    # Then the valid post still got through
    assert feed_ids(client, "bob") == [post["id"]]
