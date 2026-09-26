from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.models import Follow


def follows(session_factory: sessionmaker[Session]) -> set[tuple[str, str]]:
    with session_factory() as session:
        return {(f.follower_id, f.followee_id) for f in session.scalars(select(Follow))}


def test_follow_creates_relationship(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    response = client.put("/users/bob/following/alice")

    assert response.status_code == 204
    assert follows(session_factory) == {("bob", "alice")}


def test_following_twice_is_idempotent(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    client.put("/users/bob/following/alice")

    response = client.put("/users/bob/following/alice")

    assert response.status_code == 204
    assert follows(session_factory) == {("bob", "alice")}


def test_cannot_follow_yourself(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    response = client.put("/users/bob/following/bob")

    assert response.status_code == 400
    assert follows(session_factory) == set()


def test_unfollow_removes_relationship(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    client.put("/users/bob/following/alice")
    client.put("/users/carol/following/alice")

    response = client.delete("/users/bob/following/alice")

    assert response.status_code == 204
    assert follows(session_factory) == {("carol", "alice")}
