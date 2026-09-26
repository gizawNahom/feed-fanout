"""
Integration-test fixtures: real Postgres and Kafka in throwaway containers.

- One Postgres and one Kafka container per test session (started once, reused).
- Every test gets empty tables and its own fresh Kafka topic, so tests never see each other's data.
- Your dev containers (docker compose) are never touched.
"""

import gc
import socket
import time
import uuid
from collections.abc import Iterator

import pytest
from alembic import command
from alembic.config import Config
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer

from feed_fanout.api import app, get_publisher, get_session
from feed_fanout.kafka import PostCreatedPublisher
from feed_fanout.worker import Outcome, make_consumer, process

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


@pytest.fixture(scope="session")
def kafka_admin(kafka_bootstrap: str) -> Iterator[AdminClient]:
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    yield admin
    # AdminClient has no close(); drop it before the container stops so it doesn't keep reconnecting.
    del admin
    gc.collect()


@pytest.fixture
def topic(kafka_admin: AdminClient) -> str:
    name = f"post-created-{uuid.uuid4().hex[:8]}"
    for future in kafka_admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)]).values():
        future.result(timeout=30)
    return name


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


class Worker:
    """Drives the real worker code one message at a time, so tests control timing."""

    def __init__(self, consumer: Consumer, session_factory: sessionmaker[Session],
                 publisher: PostCreatedPublisher) -> None:
        self.consumer = consumer
        self.session_factory = session_factory
        self.publisher = publisher

    def process_next(self, timeout: float = 20) -> Outcome:
        self.publisher.flush(10)  # make sure anything the API published has reached Kafka
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = self.consumer.poll(0.5)
            if msg is not None and not msg.error():
                return process(msg, self.consumer, self.session_factory)
        raise AssertionError(f"no message arrived within {timeout}s")


@pytest.fixture
def worker(kafka_bootstrap: str, topic: str, session_factory: sessionmaker[Session],
           publisher: PostCreatedPublisher) -> Iterator[Worker]:
    consumer = make_consumer(kafka_bootstrap, group_id=f"test-{uuid.uuid4().hex[:8]}")
    consumer.subscribe([topic])
    yield Worker(consumer, session_factory, publisher)
    consumer.close()
