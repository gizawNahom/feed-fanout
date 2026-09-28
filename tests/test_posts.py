import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from sqlalchemy import select
from sqlalchemy.exc import DBAPIError

from feed_fanout.api import app, get_post_created_topic
from feed_fanout.events import PostCreated
from feed_fanout.models import OutboxEvent, Post


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


def test_create_post_writes_event_to_outbox(client: TestClient, session_factory: sessionmaker[Session],
                                             topic: str) -> None:
    post = client.post("/posts", json={"author_id": "alice", "text": "hello"}).json()

    with session_factory() as session:
        [row] = session.scalars(select(OutboxEvent)).all()
    assert row.topic == topic
    assert row.key == "alice"
    event = PostCreated.model_validate_json(row.payload)
    assert str(event.post_id) == post["id"]
    assert event.author_id == "alice"


def test_post_and_event_are_saved_atomically(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    # Given the outbox insert will fail (topic name longer than the column allows)
    app.dependency_overrides[get_post_created_topic] = lambda: "x" * 300

    # When a post is created, the request fails...
    with pytest.raises(DBAPIError):
        client.post("/posts", json={"author_id": "alice", "text": "hello"})

    # ...and the post was rolled back with it: never a post without its event.
    with session_factory() as session:
        assert session.scalars(select(Post)).all() == []
        assert session.scalars(select(OutboxEvent)).all() == []


@pytest.mark.parametrize("body", [
    {"author_id": "alice", "text": ""},
    {"author_id": "", "text": "hello"},
    {"author_id": "alice"},
    {"author_id": "alice", "text": "x" * 5001},
])
def test_invalid_post_is_rejected(client: TestClient, body: dict[str, str],
                                  session_factory: sessionmaker[Session]) -> None:
    assert client.post("/posts", json=body).status_code == 422
    with session_factory() as session:
        assert session.scalars(select(OutboxEvent)).all() == []
