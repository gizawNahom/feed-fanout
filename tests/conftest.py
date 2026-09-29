"""
Integration-test fixtures: real Postgres and Kafka in throwaway containers.

- One Postgres and one Kafka container per test session (started once, reused).
- Every test gets empty tables and its own fresh Kafka topic, so tests never see each other's data.
- Your dev containers (docker compose) are never touched.
"""

import socket
import time
import uuid
from collections.abc import Iterator

import pytest
from alembic import command
from alembic.config import Config
from confluent_kafka import Consumer, Message, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]  # stubs lack the re-export
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer

from feed_fanout.adapters.auth import StaticKey, TokenVerifier
from feed_fanout.adapters.http import app, get_session, get_token_verifier, get_unit_of_work
from feed_fanout.adapters.kafka.producers import DeadLetterPublisher, make_producer
from feed_fanout.adapters.kafka.relay import OutboxRelay
from feed_fanout.adapters.kafka.worker import FanoutWorker, Outcome, make_consumer
from feed_fanout.adapters.postgres.repositories import PostgresUnitOfWork
from feed_fanout.application.use_cases import FanOutPost
from feed_fanout.retry import RetryPolicy
from tests import auth_keys

# The API trusts tokens from the test identity provider (tests/auth_keys.py).
TEST_VERIFIER = TokenVerifier(StaticKey(auth_keys.PUBLIC_KEY), issuer=auth_keys.ISSUER, audience=auth_keys.AUDIENCE)

KAFKA_IMAGE = "apache/kafka:4.3.1"  # same images as docker-compose.yml
POSTGRES_IMAGE = "postgres:18.6-alpine"


# --- Containers (once per session) -------------------------------------------


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as pg:
        url = pg.get_connection_url()
        # Build the schema with the real migrations, so the migrations are tested too.
        cfg = Config("alembic.ini")
        cfg.set_main_option("sqlalchemy.url", url)
        command.upgrade(cfg, "head")
        yield url


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        port: int = s.getsockname()[1]
        return port


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    # Kafka tells clients which address to connect to (the advertised listener), so the
    # container must listen on the same port it's published on. Pick a free one.
    port = _free_port()
    container = (
        DockerContainer(KAFKA_IMAGE)
        .with_bind_ports(port, port)
        .with_envs(
            KAFKA_NODE_ID="1",
            KAFKA_PROCESS_ROLES="broker,controller",
            KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9093",
            KAFKA_LISTENERS=f"PLAINTEXT://:{port},CONTROLLER://:9093",
            KAFKA_ADVERTISED_LISTENERS=f"PLAINTEXT://localhost:{port}",
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
            KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER",
            KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR="1",
            KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR="1",
            KAFKA_TRANSACTION_STATE_LOG_MIN_ISR="1",
            KAFKA_AUTO_CREATE_TOPICS_ENABLE="false",
            # Default is 3s: every new consumer group would wait 3s before starting.
            KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS="0",
            # The new consumer protocol (KIP-848) assigns partitions on heartbeats, 5s apart by
            # default. Faster heartbeats make tests that start consumers much quicker.
            KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS="500",
            KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS="500",
        )
    )
    with container:
        bootstrap = f"localhost:{port}"
        _wait_for_kafka(bootstrap)
        yield bootstrap


def _wait_for_kafka(bootstrap: str, timeout: float = 60) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + timeout
    while True:
        try:
            admin.list_topics(timeout=2)
            return
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.5)


# --- Per-test fixtures -------------------------------------------------------


@pytest.fixture(scope="session")
def engine(postgres_url: str) -> Iterator[Engine]:
    engine = create_engine(postgres_url)
    yield engine
    engine.dispose()


@pytest.fixture
def session_factory(engine: Engine) -> Iterator[sessionmaker[Session]]:
    yield sessionmaker(engine, expire_on_commit=False)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE feed_items, follows, posts, outbox, posting_buckets"))


def _create_topic(bootstrap: str, name: str, partitions: int) -> str:
    # A short-lived AdminClient: it has no close(), so one kept around (e.g. in a session
    # fixture) would keep reconnecting after the Kafka container stops.
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for future in admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)]).values():
        future.result(timeout=30)
    return name


@pytest.fixture
def topic(kafka_bootstrap: str) -> str:
    return _create_topic(kafka_bootstrap, f"post-created-{uuid.uuid4().hex[:8]}", partitions=3)


@pytest.fixture
def dlt_topic(kafka_bootstrap: str, topic: str) -> str:
    return _create_topic(kafka_bootstrap, f"{topic}.dlt", partitions=1)


@pytest.fixture
def dead_letters(kafka_bootstrap: str, dlt_topic: str) -> Iterator[DeadLetterPublisher]:
    publisher = DeadLetterPublisher(kafka_bootstrap, dlt_topic)
    yield publisher
    publisher.close()


class TopicReader:
    """Reads messages from one topic with its own consumer group, for assertions."""

    def __init__(self, consumer: Consumer, topic: str) -> None:
        self.consumer = consumer
        self.topic = topic
        self.read = 0  # messages returned so far

    def __call__(self, count: int, timeout: float = 20) -> list[Message]:
        messages: list[Message] = []
        deadline = time.monotonic() + timeout
        while len(messages) < count and time.monotonic() < deadline:
            msg = self.consumer.poll(0.5)
            if msg is not None and not msg.error():
                messages.append(msg)
        assert len(messages) == count, f"expected {count} message(s), got {len(messages)}"
        self.read += count
        return messages

    def nothing_more(self) -> None:
        """Assert the topic holds nothing beyond what was read: ask the broker how many
        messages each partition holds (its offsets) and compare. Exact, with no waiting
        for something that might still arrive."""
        partitions = self.consumer.list_topics(self.topic, timeout=10).topics[self.topic].partitions
        held = 0
        for p in partitions:
            low, high = self.consumer.get_watermark_offsets(TopicPartition(self.topic, p), timeout=10)
            held += high - low
        assert held == self.read, f"the topic holds {held} message(s), but {self.read} were read"


def _reader(bootstrap: str, topic: str) -> Iterator[TopicReader]:
    consumer = make_consumer(bootstrap, group_id=f"test-reader-{uuid.uuid4().hex[:8]}")
    consumer.subscribe([topic])
    yield TopicReader(consumer, topic)
    consumer.close()


@pytest.fixture
def read_dead_letters(kafka_bootstrap: str, dlt_topic: str) -> Iterator[TopicReader]:
    yield from _reader(kafka_bootstrap, dlt_topic)


@pytest.fixture
def read_topic(kafka_bootstrap: str, topic: str) -> Iterator[TopicReader]:
    yield from _reader(kafka_bootstrap, topic)


@pytest.fixture
def relay(kafka_bootstrap: str, session_factory: sessionmaker[Session]) -> Iterator[OutboxRelay]:
    # Short message timeout so failure tests don't wait 10s.
    producer = make_producer(kafka_bootstrap, message_timeout_ms=3000)
    relay = OutboxRelay(session_factory, producer, flush_timeout=5)
    yield relay
    relay.close()


@pytest.fixture
def client(session_factory: sessionmaker[Session], topic: str) -> Iterator[TestClient]:
    """The FastAPI app, wired to the test database. Its outbox events target the test topic."""

    def test_session() -> Iterator[Session]:
        with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = test_session
    app.dependency_overrides[get_token_verifier] = lambda: TEST_VERIFIER
    app.dependency_overrides[get_unit_of_work] = lambda: lambda: PostgresUnitOfWork(session_factory, topic)
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def postgres_fan_out(session_factory: sessionmaker[Session]) -> FanOutPost:
    """The fanout use case on the test database. (Fanout writes no outbox events: topic unused.)"""
    return FanOutPost(lambda: PostgresUnitOfWork(session_factory, post_created_topic="unused"))


# Fast backoff so retry tests take milliseconds, not seconds.
FAST_RETRY = RetryPolicy(base_delay=0.01, max_delay=0.05, max_unexpected_attempts=3)


class Worker:
    """Drives the real FanoutWorker one message at a time, so tests control timing."""

    def __init__(self, fanout: FanoutWorker, relay: OutboxRelay) -> None:
        self.fanout = fanout
        self.relay = relay

    def process_next(self, timeout: float = 20) -> Outcome:
        self.relay.publish_batch()  # move anything the API wrote to the outbox into Kafka
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = self.fanout.consumer.poll(0.5)
            if msg is not None and not msg.error():
                return self.fanout.process(msg)
        raise AssertionError(f"no message arrived within {timeout}s")


@pytest.fixture
def worker(
    kafka_bootstrap: str,
    topic: str,
    session_factory: sessionmaker[Session],
    relay: OutboxRelay,
    dead_letters: DeadLetterPublisher,
) -> Iterator[Worker]:
    consumer = make_consumer(kafka_bootstrap, group_id=f"test-{uuid.uuid4().hex[:8]}")
    consumer.subscribe([topic])
    yield Worker(FanoutWorker(consumer, postgres_fan_out(session_factory), dead_letters, FAST_RETRY), relay)
    consumer.close()
