import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.api import app, get_post_created_topic
from feed_fanout.events import PostCreated
from feed_fanout.models import OutboxEvent, Post


def test_create_post_writes_event_to_outbox(
    client: TestClient, session_factory: sessionmaker[Session], topic: str
) -> None:
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
