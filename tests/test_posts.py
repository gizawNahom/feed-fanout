import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.models import Post
from tests.conftest import Worker


def test_create_post_saves_it(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    response = client.post("/posts", json={"author_id": "alice", "text": "hello"})

    assert response.status_code == 201
    body = response.json()
    assert body["author_id"] == "alice"
    assert body["text"] == "hello"
    with session_factory() as session:
        saved = session.get(Post, uuid.UUID(body["id"]))
    assert saved is not None
    assert saved.text == "hello"


def test_create_post_publishes_event(client: TestClient, worker: Worker) -> None:
    client.post("/posts", json={"author_id": "alice", "text": "hello"})

    # The worker receiving and handling a message proves the API published one.
    worker.process_next()


@pytest.mark.parametrize("body", [
    {"author_id": "alice", "text": ""},
    {"author_id": "", "text": "hello"},
    {"author_id": "alice"},
    {"author_id": "alice", "text": "x" * 5001},
])
def test_invalid_post_is_rejected(client: TestClient, body: dict[str, str]) -> None:
    assert client.post("/posts", json=body).status_code == 422
