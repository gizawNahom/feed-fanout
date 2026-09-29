from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.relay import OutboxRelay
from tests.acceptance.contract import System
from tests.acceptance.full import FullSystem
from tests.conftest import Worker


@pytest.fixture(params=["full"])
def system(request: pytest.FixtureRequest) -> Iterator[System]:
    """Every acceptance scenario runs once per assembly."""
    if request.param == "full":
        client: TestClient = request.getfixturevalue("client")
        relay: OutboxRelay = request.getfixturevalue("relay")
        worker: Worker = request.getfixturevalue("worker")
        session_factory: sessionmaker[Session] = request.getfixturevalue("session_factory")
        yield FullSystem(client, relay, worker, session_factory)
