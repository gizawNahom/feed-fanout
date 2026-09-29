"""Reading feeds: ordering and keyset pagination."""

import uuid
from datetime import UTC, datetime

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.adapters.postgres.models import FeedItem, Post

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


def test_pagination_handles_posts_with_identical_timestamps(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    # Ties on created_at are broken by post_id, so no post is skipped or repeated across pages.
    ids = seed_feed(session_factory, "bob", [T0] * 5)

    pages = walk_pages(client, "bob", limit=2)

    flat = [i for page in pages for i in page]
    assert sorted(flat) == sorted(ids)
    assert len(flat) == len(set(flat))
