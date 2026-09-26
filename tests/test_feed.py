"""Reading feeds: ordering and keyset pagination."""

import uuid
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.models import FeedItem, Post
from tests.helpers import feed_ids

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def seed_feed(session_factory: sessionmaker[Session], user: str, created_ats: list[datetime]) -> list[str]:
    """Put posts straight into a user's feed (bypassing the worker) to test reads in isolation."""
    ids = []
    with session_factory.begin() as session:
        for ts in created_ats:
            post = Post(id=uuid.uuid4(), author_id="alice", text="x", created_at=ts)
            session.add(post)
            session.flush()
            session.add(FeedItem(user_id=user, post_id=post.id, created_at=ts))
            ids.append(str(post.id))
    return ids


def walk_pages(client: TestClient, user: str, limit: int) -> list[list[str]]:
    pages, cursor = [], None
    while True:
        params: dict[str, str | int] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        body = client.get(f"/users/{user}/feed", params=params).json()
        pages.append([item["id"] for item in body["items"]])
        cursor = body["next_cursor"]
        if cursor is None:
            return pages


def test_feed_is_newest_first(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    oldest, middle, newest = seed_feed(session_factory, "bob", [T0, T0 + timedelta(seconds=1), T0 + timedelta(seconds=2)])

    assert feed_ids(client, "bob") == [newest, middle, oldest]


def test_empty_feed(client: TestClient) -> None:
    body = client.get("/users/nobody/feed").json()

    assert body == {"items": [], "next_cursor": None}


def test_pagination_returns_every_post_exactly_once(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    ids = seed_feed(session_factory, "bob", [T0 + timedelta(seconds=i) for i in range(7)])

    pages = walk_pages(client, "bob", limit=3)

    assert [len(p) for p in pages] == [3, 3, 1]
    assert [i for page in pages for i in page] == list(reversed(ids))


def test_pagination_handles_posts_with_identical_timestamps(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    # Ties on created_at are broken by post_id, so no post is skipped or repeated across pages.
    ids = seed_feed(session_factory, "bob", [T0] * 5)

    pages = walk_pages(client, "bob", limit=2)

    flat = [i for page in pages for i in page]
    assert sorted(flat) == sorted(ids)
    assert len(flat) == len(set(flat))


def test_no_next_cursor_when_page_is_exactly_full(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    seed_feed(session_factory, "bob", [T0, T0 + timedelta(seconds=1)])

    body = client.get("/users/bob/feed", params={"limit": 2}).json()

    assert len(body["items"]) == 2
    assert body["next_cursor"] is None


def test_invalid_cursor_is_rejected(client: TestClient) -> None:
    assert client.get("/users/bob/feed", params={"cursor": "not-a-cursor"}).status_code == 400


def test_limit_is_bounded(client: TestClient) -> None:
    assert client.get("/users/bob/feed", params={"limit": 0}).status_code == 422
    assert client.get("/users/bob/feed", params={"limit": 101}).status_code == 422
