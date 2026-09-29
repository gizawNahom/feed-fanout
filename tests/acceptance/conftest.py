from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.adapters.kafka.relay import OutboxRelay
from tests.acceptance.contract import System
from tests.acceptance.full import FullSystem
from tests.acceptance.memory import MemorySystem
from tests.conftest import Worker


@pytest.fixture(params=["memory", "full"])
def system(request: pytest.FixtureRequest) -> Iterator[System]:
    """Every acceptance scenario runs once per assembly.

    memory: domain + use cases, in-memory adapters. Microseconds, no Docker.
    full:   the real stack (HTTP, Postgres, outbox, Kafka, worker). Seconds.
    Run only the fast one with: pytest tests/acceptance -k memory
    """
    if request.param == "memory":
        yield MemorySystem()
    elif request.param == "full":
        client: TestClient = request.getfixturevalue("client")
        relay: OutboxRelay = request.getfixturevalue("relay")
        worker: Worker = request.getfixturevalue("worker")
        session_factory: sessionmaker[Session] = request.getfixturevalue("session_factory")
        yield FullSystem(client, relay, worker, session_factory)
