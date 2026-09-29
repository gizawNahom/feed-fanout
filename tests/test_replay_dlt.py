"""Replaying dead letters: inspect, fix the cause, replay, and never loop forever."""

import uuid
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.adapters.kafka.events import PostCreated
from feed_fanout.adapters.kafka.producers import close_producer, make_producer
from feed_fanout.adapters.kafka.replay_dlt import main, read_dead_letters, replay
from feed_fanout.adapters.kafka.worker import Outcome
from feed_fanout.adapters.postgres.models import FeedItem, Follow, Post
from tests.conftest import TopicReader, Worker
from tests.helpers import follow
from tests.test_fanout import produce_raw


@pytest.fixture
def producer(kafka_bootstrap: str):
    producer = make_producer(kafka_bootstrap)
    yield producer
    close_producer(producer)


def feed_rows(session_factory: sessionmaker[Session]) -> int:
    with session_factory() as session:
        return session.scalar(select(func.count()).select_from(FeedItem)) or 0


def dead_letter_unknown_post(worker: Worker, kafka_bootstrap: str, topic: str) -> PostCreated:
    """An event for a post that doesn't exist yet -> dead-lettered as unknown_post.

    Only detected when the author has followers: with nobody to fan out to, no feed row is
    inserted, so the foreign key is never checked (and there's nothing to do anyway).
    """
    with worker.relay.session_factory.begin() as session:
        session.execute(insert(Follow).values(follower_id="bob", followee_id="alice").on_conflict_do_nothing())
    event = PostCreated(post_id=uuid.uuid4(), author_id="alice", created_at=datetime.now(UTC))
    produce_raw(kafka_bootstrap, topic, key="alice", value=event.model_dump_json().encode())
    assert worker.process_next() == Outcome.DEAD_LETTERED
    return event


def dead_letter_garbage(worker: Worker, kafka_bootstrap: str, topic: str) -> None:
    produce_raw(kafka_bootstrap, topic, key="alice", value=b"garbage")
    assert worker.process_next() == Outcome.DEAD_LETTERED


def fix_by_creating(session_factory: sessionmaker[Session], event: PostCreated) -> None:
    with session_factory.begin() as session:
        session.add(Post(id=event.post_id, author_id=event.author_id, text="restored", created_at=event.created_at))


def test_replay_after_fixing_the_cause_fans_out(
    client: TestClient,
    worker: Worker,
    producer,
    session_factory: sessionmaker[Session],
    kafka_bootstrap: str,
    topic: str,
    dlt_topic: str,
) -> None:
    # Given a dead-lettered event whose cause has since been fixed
    follow(client, "bob", "alice")
    event = dead_letter_unknown_post(worker, kafka_bootstrap, topic)
    fix_by_creating(session_factory, event)

    # When it is replayed
    result = replay(read_dead_letters(kafka_bootstrap, dlt_topic), producer, execute=True)

    # Then it goes back through the normal pipeline and reaches bob's feed
    assert len(result.replayed) == 1
    assert worker.process_next() == Outcome.FANNED_OUT
    assert feed_rows(session_factory) == 1


def test_dry_run_changes_nothing(
    worker: Worker,
    producer,
    read_topic: TopicReader,
    kafka_bootstrap: str,
    topic: str,
    dlt_topic: str,
) -> None:
    dead_letter_garbage(worker, kafka_bootstrap, topic)
    read_topic(1)  # the original garbage message

    result = replay(read_dead_letters(kafka_bootstrap, dlt_topic), producer)  # execute=False

    assert len(result.replayed) == 1  # reported...
    read_topic.nothing_more()  # ...but not sent


def test_filter_by_reason(
    worker: Worker,
    producer,
    kafka_bootstrap: str,
    topic: str,
    dlt_topic: str,
) -> None:
    dead_letter_garbage(worker, kafka_bootstrap, topic)
    dead_letter_unknown_post(worker, kafka_bootstrap, topic)

    result = replay(read_dead_letters(kafka_bootstrap, dlt_topic), producer, reasons={"unknown_post"})

    assert [d.reason for d in result.replayed] == ["unknown_post"]


def test_filter_by_offset_range(worker: Worker, kafka_bootstrap: str, topic: str, dlt_topic: str) -> None:
    for _ in range(4):
        dead_letter_garbage(worker, kafka_bootstrap, topic)

    letters = read_dead_letters(kafka_bootstrap, dlt_topic, from_offset=1, to_offset=2)

    assert [d.offset for d in letters] == [1, 2]


def test_failing_again_returns_with_count_and_clean_headers(
    worker: Worker,
    producer,
    kafka_bootstrap: str,
    topic: str,
    dlt_topic: str,
) -> None:
    # Given a dead letter that is still broken
    dead_letter_garbage(worker, kafka_bootstrap, topic)

    # When it is replayed and fails again
    replay(read_dead_letters(kafka_bootstrap, dlt_topic), producer, execute=True)
    assert worker.process_next() == Outcome.DEAD_LETTERED

    # Then it is back in the dead-letter topic, carrying its replay count, with one set of dlt headers
    second = read_dead_letters(kafka_bootstrap, dlt_topic, from_offset=1)[0]
    assert second.replay_count == 1
    names = [k for k, _ in second.headers]
    assert names.count("dlt.reason") == 1
    assert names.count("dlt.original.offset") == 1


def test_replay_limit_stops_loops_unless_forced(
    worker: Worker,
    producer,
    kafka_bootstrap: str,
    topic: str,
    dlt_topic: str,
) -> None:
    # Given a still-broken event that has bounced back max_replays times
    dead_letter_garbage(worker, kafka_bootstrap, topic)
    for i in range(2):
        latest = read_dead_letters(kafka_bootstrap, dlt_topic, from_offset=i)[:1]
        replay(latest, producer, max_replays=2, execute=True)
        worker.process_next()
    [exhausted] = read_dead_letters(kafka_bootstrap, dlt_topic, from_offset=2)
    assert exhausted.replay_count == 2

    # Then the tool refuses to replay it again...
    refused = replay([exhausted], producer, max_replays=2, execute=True)
    assert refused.replayed == [] and refused.over_limit == [exhausted]

    # ...unless forced
    assert len(replay([exhausted], producer, max_replays=2, force=True).replayed) == 1


def test_replaying_twice_adds_no_duplicates(
    client: TestClient,
    worker: Worker,
    producer,
    session_factory: sessionmaker[Session],
    kafka_bootstrap: str,
    topic: str,
    dlt_topic: str,
) -> None:
    follow(client, "bob", "alice")
    event = dead_letter_unknown_post(worker, kafka_bootstrap, topic)
    fix_by_creating(session_factory, event)
    letters = read_dead_letters(kafka_bootstrap, dlt_topic)

    replay(letters, producer, execute=True)
    replay(letters, producer, execute=True)  # an operator runs it twice by mistake
    worker.process_next()
    worker.process_next()

    assert feed_rows(session_factory) == 1


def test_cli_dry_run_summary(
    worker: Worker,
    kafka_bootstrap: str,
    topic: str,
    dlt_topic: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    dead_letter_garbage(worker, kafka_bootstrap, topic)
    dead_letter_unknown_post(worker, kafka_bootstrap, topic)

    assert main(["--topic", dlt_topic, "--bootstrap-servers", kafka_bootstrap]) == 0

    out = capsys.readouterr().out
    assert "2 dead letter(s)" in out
    assert "malformed=1" in out and "unknown_post=1" in out
    assert "2 would replay" in out and "dry run" in out


def test_empty_dead_letter_topic(kafka_bootstrap: str, dlt_topic: str) -> None:
    assert read_dead_letters(kafka_bootstrap, dlt_topic) == []
