"""Graceful shutdown and rebalances: stopping or adding workers never loses or stalls events."""

import contextlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from confluent_kafka import ConsumerGroupTopicPartitions, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.adapters.kafka.events import PostCreated
from feed_fanout.adapters.kafka.producers import DeadLetterPublisher
from feed_fanout.adapters.kafka.relay import OutboxRelay
from feed_fanout.adapters.kafka.worker import FanoutWorker, Outcome, make_consumer
from feed_fanout.adapters.postgres.models import FeedItem
from tests.conftest import FAST_RETRY, postgres_fan_out
from tests.helpers import create_post, follow


def wait_until(condition: Callable[[], bool], timeout: float = 30, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.2)


def feed_rows(session_factory: sessionmaker[Session]) -> int:
    with session_factory() as session:
        return session.scalar(select(func.count()).select_from(FeedItem)) or 0


class Group:
    """Inspects a consumer group from the outside, like an operator would."""

    def __init__(self, bootstrap: str, group_id: str, topic: str) -> None:
        self.admin = AdminClient({"bootstrap.servers": bootstrap})
        self.group_id = group_id
        self.topic = topic

    def members(self) -> int:
        return len(self.admin.describe_consumer_groups([self.group_id])[self.group_id].result().members)

    def committed(self) -> int:
        """Total committed offset across the topic's partitions (= events marked done)."""
        request = [ConsumerGroupTopicPartitions(self.group_id)]
        result = self.admin.list_consumer_group_offsets(request)[self.group_id].result()
        return sum(tp.offset for tp in result.topic_partitions if tp.topic == self.topic and tp.offset > 0)


class WorkerProcess:
    """`python -m feed_fanout.adapters.kafka.worker` as a real OS process, so real signals can be sent to it."""

    def __init__(self, env: dict[str, str]) -> None:
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "feed_fanout.adapters.kafka.worker"],
            env={**os.environ, **env},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.lines: list[str] = []
        self.events: list[dict[str, Any]] = []
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        # Not a terminal, so the worker logs JSON lines (LOG_FORMAT=auto).
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            self.lines.append(line)
            with contextlib.suppress(json.JSONDecodeError):
                self.events.append(json.loads(line))

    def logged(self, msg: str, **fields: Any) -> bool:
        """Whether a log event with this message and these field values was emitted."""
        return any(e.get("msg") == msg and all(e.get(k) == v for k, v in fields.items()) for e in self.events)

    def sigterm_and_wait(self, timeout: float = 15) -> float:
        start = time.monotonic()
        self.proc.send_signal(signal.SIGTERM)
        self.proc.wait(timeout=timeout)
        return time.monotonic() - start

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()


@pytest.fixture
def group_id() -> str:
    return f"test-shutdown-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def spawn_worker(
    kafka_bootstrap: str, topic: str, dlt_topic: str, group_id: str
) -> Iterator[Callable[..., WorkerProcess]]:
    processes: list[WorkerProcess] = []

    def spawn(database_url: str) -> WorkerProcess:
        p = WorkerProcess(
            {
                "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap,
                "DATABASE_URL": database_url,
                "POST_CREATED_TOPIC": topic,  # its dead-letter topic is f"{topic}.dlt" = dlt_topic
                "FANOUT_GROUP_ID": group_id,
            }
        )
        processes.append(p)
        return p

    yield spawn
    for p in processes:
        p.kill()


# --- SIGTERM ------------------------------------------------------------------


def test_sigterm_exits_promptly_commits_and_leaves_the_group(
    client: TestClient,
    relay: OutboxRelay,
    spawn_worker,
    postgres_url: str,
    session_factory: sessionmaker[Session],
    kafka_bootstrap: str,
    topic: str,
    group_id: str,
) -> None:
    # Given a running worker that has fanned out one post
    follow(client, "bob", "alice")
    create_post(client, "alice")
    relay.publish_batch()
    worker = spawn_worker(postgres_url)
    wait_until(lambda: feed_rows(session_factory) == 1, what="fanout")
    group = Group(kafka_bootstrap, group_id, topic)
    assert group.members() == 1

    # When it receives SIGTERM (what docker stop / Kubernetes send)
    elapsed = worker.sigterm_and_wait()

    # Then it exits cleanly and quickly, its work is committed, and it has left the group,
    # so its partitions are free right away instead of after the ~45s session timeout.
    assert worker.proc.returncode == 0
    assert elapsed < 5
    assert worker.logged("closed")
    assert group.committed() == 1
    assert group.members() == 0


def test_sigterm_during_retries_exits_promptly_without_committing(
    spawn_worker,
    kafka_bootstrap: str,
    topic: str,
    group_id: str,
) -> None:
    # Given a worker stuck retrying an event because the database is unreachable
    from tests.test_fanout import produce_raw

    event = PostCreated(post_id=uuid.uuid4(), author_id="alice", created_at="2026-01-01T00:00:00Z")
    produce_raw(kafka_bootstrap, topic, key="alice", value=event.model_dump_json().encode())
    worker = spawn_worker("postgresql+psycopg://nobody:nothing@localhost:1/none")  # nothing listens here
    wait_until(lambda: worker.logged("attempt failed, retrying", kind="transient"), what="retrying")

    # When it receives SIGTERM, even mid-backoff
    elapsed = worker.sigterm_and_wait()

    # Then it stops retrying and exits promptly, without marking the event done:
    # it will be re-delivered, not lost.
    assert worker.proc.returncode == 0
    assert elapsed < 5
    assert worker.logged("abandoning retries for shutdown")
    assert Group(kafka_bootstrap, group_id, topic).committed() == 0


def test_second_sigterm_forces_exit(spawn_worker, postgres_url: str) -> None:
    worker = spawn_worker(postgres_url)
    wait_until(lambda: worker.logged("consuming"), what="startup")

    worker.proc.send_signal(signal.SIGTERM)
    worker.proc.send_signal(signal.SIGTERM)

    worker.proc.wait(timeout=15)  # exits either way; must not hang


# --- In-process stop (relay and worker) ---------------------------------------


def test_relay_stops_promptly(relay: OutboxRelay) -> None:
    thread = threading.Thread(target=relay.run, kwargs={"idle_interval": 30})
    thread.start()
    time.sleep(0.5)  # now idle, waiting up to 30s for new events

    relay.stop()

    thread.join(timeout=3)
    assert not thread.is_alive()


# --- Rebalances ---------------------------------------------------------------


class RejectingCommits:
    """A consumer whose commits fail as if the partition had just been reassigned."""

    def __init__(self, consumer, code: int) -> None:
        self._consumer = consumer
        self._code = code

    def commit(self, *args, **kwargs):
        raise KafkaException(KafkaError(self._code))

    def __getattr__(self, name):
        return getattr(self._consumer, name)


@pytest.mark.parametrize("code", [KafkaError.STALE_MEMBER_EPOCH, KafkaError.ILLEGAL_GENERATION])
def test_commit_rejected_after_reassignment_does_not_crash(
    code: int,
    client: TestClient,
    worker,
    session_factory: sessionmaker[Session],
) -> None:
    # The event was fanned out, but the partition moved before the commit. The new owner
    # will re-process it; fanout is idempotent, so the worker just logs and carries on.
    follow(client, "bob", "alice")
    create_post(client, "alice")
    worker.fanout.consumer = RejectingCommits(worker.fanout.consumer, code)

    assert worker.process_next() == Outcome.FANNED_OUT
    assert feed_rows(session_factory) == 1


def test_unrelated_commit_error_still_raises(client: TestClient, worker) -> None:
    create_post(client, "alice")
    worker.fanout.consumer = RejectingCommits(worker.fanout.consumer, KafkaError._TRANSPORT)

    with pytest.raises(KafkaException):
        worker.process_next()


def test_worker_joining_mid_stream_loses_nothing(
    client: TestClient,
    relay: OutboxRelay,
    session_factory: sessionmaker[Session],
    kafka_bootstrap: str,
    topic: str,
    dlt_topic: str,
    group_id: str,
) -> None:
    # Given one worker consuming, with posts from many authors (spread over all partitions)
    authors = [f"author{i}" for i in range(12)]
    for a in authors:
        follow(client, "reader", a)

    def start_worker() -> tuple[FanoutWorker, threading.Thread]:
        w = FanoutWorker(
            make_consumer(kafka_bootstrap, group_id),
            postgres_fan_out(session_factory),
            DeadLetterPublisher(kafka_bootstrap, dlt_topic),
            FAST_RETRY,
        )
        t = threading.Thread(target=w.run, args=(topic,))
        t.start()
        return w, t

    first = start_worker()
    for a in authors[:6]:
        create_post(client, a)
    relay.publish_batch()

    # When a second worker joins (a rebalance) while more posts arrive
    second = start_worker()
    group = Group(kafka_bootstrap, group_id, topic)
    wait_until(lambda: group.members() == 2, what="second worker to join")
    for a in authors[6:]:
        create_post(client, a)
    relay.publish_batch()

    # Then every post is fanned out exactly once
    try:
        wait_until(lambda: feed_rows(session_factory) == len(authors), what="all posts fanned out")
        time.sleep(1)
        assert feed_rows(session_factory) == len(authors)
    finally:
        for w, t in (first, second):
            w.stop()
            t.join(timeout=10)
