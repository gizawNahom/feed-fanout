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
from confluent_kafka import Consumer, Message
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer

from feed_fanout.api import app, get_publisher, get_session
from feed_fanout.kafka import DeadLetterPublisher, PostCreatedPublisher
from feed_fanout.worker import FanoutWorker, Outcome, RetryPolicy, make_consumer

KAFKA_IMAGE = "apache/kafka:4.3.1"       # same images as docker-compose.yml
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
        return s.getsockname()[1]


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
        conn.execute(text("TRUNCATE feed_items, follows, posts"))


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


@pytest.fixture
def read_dead_letters(kafka_bootstrap: str, dlt_topic: str) -> Iterator["DeadLetterReader"]:
    consumer = make_consumer(kafka_bootstrap, group_id=f"test-dlt-{uuid.uuid4().hex[:8]}")
    consumer.subscribe([dlt_topic])
    yield DeadLetterReader(consumer)
    consumer.close()


class DeadLetterReader:
    def __init__(self, consumer: Consumer) -> None:
        self.consumer = consumer

    def __call__(self, count: int, timeout: float = 20) -> list[Message]:
        messages: list[Message] = []
        deadline = time.monotonic() + timeout
        while len(messages) < count and time.monotonic() < deadline:
            msg = self.consumer.poll(0.5)
            if msg is not None and not msg.error():
                messages.append(msg)
        assert len(messages) == count, f"expected {count} dead letters, got {len(messages)}"
        return messages


@pytest.fixture
def publisher(kafka_bootstrap: str, topic: str) -> Iterator[PostCreatedPublisher]:
    publisher = PostCreatedPublisher(kafka_bootstrap, topic)
    yield publisher
    publisher.close()


@pytest.fixture
def client(session_factory: sessionmaker[Session], publisher: PostCreatedPublisher) -> Iterator[TestClient]:
    """The FastAPI app, wired to the test database and test topic."""
    def test_session() -> Iterator[Session]:
        with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = test_session
    app.dependency_overrides[get_publisher] = lambda: publisher
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


# Fast backoff so retry tests take milliseconds, not seconds.
FAST_RETRY = RetryPolicy(base_delay=0.01, max_delay=0.05, max_unexpected_attempts=3)


class Worker:
    """Drives the real FanoutWorker one message at a time, so tests control timing."""

    def __init__(self, fanout: FanoutWorker, publisher: PostCreatedPublisher) -> None:
        self.fanout = fanout
        self.publisher = publisher

    def process_next(self, timeout: float = 20) -> Outcome:
        self.publisher.flush(10)  # make sure anything the API published has reached Kafka
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = self.fanout.consumer.poll(0.5)
            if msg is not None and not msg.error():
                return self.fanout.process(msg)
        raise AssertionError(f"no message arrived within {timeout}s")


@pytest.fixture
def worker(kafka_bootstrap: str, topic: str, session_factory: sessionmaker[Session],
           publisher: PostCreatedPublisher, dead_letters: DeadLetterPublisher) -> Iterator[Worker]:
    consumer = make_consumer(kafka_bootstrap, group_id=f"test-{uuid.uuid4().hex[:8]}")
    consumer.subscribe([topic])
    yield Worker(FanoutWorker(consumer, session_factory, dead_letters, FAST_RETRY), publisher)
    consumer.close()
