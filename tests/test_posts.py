import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.adapters.http import app, get_unit_of_work
from feed_fanout.adapters.kafka.events import PostCreated
from feed_fanout.adapters.postgres.models import OutboxEvent, Post
from feed_fanout.adapters.postgres.repositories import PostgresUnitOfWork
from tests.auth_keys import auth_header


def test_create_post_writes_event_to_outbox(
    client: TestClient, session_factory: sessionmaker[Session], topic: str
) -> None:
    post = client.post("/posts", json={"text": "hello"}, headers=auth_header("alice")).json()

    with session_factory() as session:
        [row] = session.scalars(select(OutboxEvent)).all()
    assert row.topic == topic
    assert row.key == "alice"
    event = PostCreated.model_validate_json(row.payload)
    assert str(event.post_id) == post["id"]
    assert event.author_id == "alice"


def test_post_and_event_are_saved_atomically(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    # Given the outbox insert will fail (topic name longer than the column allows)
    app.dependency_overrides[get_unit_of_work] = lambda: lambda: PostgresUnitOfWork(session_factory, "x" * 300)

    # When a post is created, the request fails...
    with pytest.raises(DBAPIError):
        client.post("/posts", json={"text": "hello"}, headers=auth_header("alice"))

    # ...and the post was rolled back with it: never a post without its event.
    with session_factory() as session:
        assert session.scalars(select(Post)).all() == []
        assert session.scalars(select(OutboxEvent)).all() == []
