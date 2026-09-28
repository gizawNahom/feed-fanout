"""The outbox relay: moves events from Postgres to Kafka without losing any."""

from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.events import PostCreated
from feed_fanout.models import OutboxEvent
from feed_fanout.relay import BatchResult, OutboxRelay
from tests.conftest import TopicReader, Worker
from tests.helpers import create_post, feed_ids, follow


def outbox_size(session_factory: sessionmaker[Session]) -> int:
    with session_factory() as session:
        return session.scalar(select(func.count()).select_from(OutboxEvent)) or 0


def test_relay_publishes_pending_events_and_removes_them(
    client: TestClient, relay: OutboxRelay, read_topic: TopicReader, session_factory: sessionmaker[Session]
) -> None:
    # Given two posts waiting in the outbox
    first = create_post(client, "alice", "one")
    second = create_post(client, "bob", "two")
    assert outbox_size(session_factory) == 2

    # When the relay runs
    result = relay.publish_batch()

    # Then both events are in Kafka, keyed by author, and the outbox is empty
    assert result == BatchResult(published=2, failed=0)
    published = {(m.key() or b"").decode(): PostCreated.model_validate_json(m.value() or b"") for m in read_topic(2)}
    assert str(published["alice"].post_id) == first["id"]
    assert str(published["bob"].post_id) == second["id"]
    assert outbox_size(session_factory) == 0


def test_relay_with_nothing_to_do(relay: OutboxRelay) -> None:
    assert relay.publish_batch() == BatchResult(published=0, failed=0)


def test_failed_publish_keeps_the_event_for_retry(relay: OutboxRelay, session_factory: sessionmaker[Session]) -> None:
    # Given an event whose publish will fail (its topic doesn't exist; auto-create is off)
    with session_factory.begin() as session:
        session.add(OutboxEvent(topic="no-such-topic", key="alice", payload="{}"))

    result = relay.publish_batch()

    # Then it is not lost: it stays in the outbox for the next attempt
    assert result == BatchResult(published=0, failed=1)
    assert outbox_size(session_factory) == 1


def test_batch_size_limits_one_run(
    client: TestClient, relay: OutboxRelay, read_topic: TopicReader, session_factory: sessionmaker[Session]
) -> None:
    for i in range(5):
        create_post(client, "alice", f"post {i}")
    relay.batch_size = 2

    assert relay.publish_batch().published == 2
    assert outbox_size(session_factory) == 3


def test_rows_locked_by_another_relay_are_skipped(
    client: TestClient, relay: OutboxRelay, read_topic: TopicReader, session_factory: sessionmaker[Session]
) -> None:
    # Given another relay holds a lock on the only pending event
    create_post(client, "alice")
    with session_factory() as other_relay:
        other_relay.execute(select(OutboxEvent).with_for_update())

        # When this relay runs, it skips the locked row instead of publishing it twice or waiting
        assert relay.publish_batch() == BatchResult(published=0, failed=0)
        other_relay.rollback()

    # Once released, it is published normally
    assert relay.publish_batch().published == 1
    read_topic(1)
    read_topic.nothing_more()


def test_api_does_not_depend_on_kafka() -> None:
    # Posting only writes to Postgres, so it keeps working while Kafka is down.
    # Checked structurally: the API module never imports a Kafka client.
    import feed_fanout.api as api

    source = Path(api.__file__).read_text()
    assert "confluent_kafka" not in source and "feed_fanout.kafka" not in source


def test_end_to_end_post_reaches_follower_via_outbox(client: TestClient, worker: Worker) -> None:
    follow(client, "bob", "alice")
    posts = [create_post(client, "alice", f"post {i}") for i in range(3)]

    for _ in posts:
        worker.process_next()  # runs the relay, then the worker

    assert sorted(feed_ids(client, "bob")) == sorted(p["id"] for p in posts)
