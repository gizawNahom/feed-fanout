"""
HTTP API: create posts, follow users, read feeds.

Run: uv run uvicorn feed_fanout.api:app --reload
Docs: http://localhost:8000/docs
"""

import base64
import logging
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from feed_fanout.db import SessionLocal
from feed_fanout.events import PostCreated
from feed_fanout.kafka import producer, publish_post_created
from feed_fanout.models import FeedItem, Follow, Post

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("feed_fanout.api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield
    # Messages still buffered in the producer are lost if we exit without flushing.
    remaining = producer.flush(10)
    if remaining:
        log.error("shutdown: %d message(s) were not delivered", remaining)


app = FastAPI(title="feed-fanout", lifespan=lifespan)


def get_session() -> Iterator[Session]:
    with SessionLocal() as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]
UserId = Annotated[str, Field(min_length=1, max_length=64)]


# --- Schemas ----------------------------------------------------------------

class CreatePost(BaseModel):
    author_id: UserId
    text: str = Field(min_length=1, max_length=5000)


class PostOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    author_id: str
    text: str
    created_at: datetime


class FeedPage(BaseModel):
    items: list[PostOut]
    next_cursor: str | None  # pass as ?cursor= to get the next (older) page


# --- Posts ------------------------------------------------------------------

@app.post("/posts", status_code=status.HTTP_201_CREATED)
def create_post(body: CreatePost, session: SessionDep) -> PostOut:
    post = Post(author_id=body.author_id, text=body.text)
    session.add(post)
    session.commit()

    # Known gap (fixed in v4 with an outbox): if the process dies between the commit
    # above and this publish, the post exists but is never fanned out.
    publish_post_created(PostCreated(post_id=post.id, author_id=post.author_id, created_at=post.created_at))
    log.info("post created post_id=%s author_id=%s", post.id, post.author_id)
    return PostOut.model_validate(post)


# --- Follows ----------------------------------------------------------------

@app.put("/users/{user_id}/following/{followee_id}", status_code=status.HTTP_204_NO_CONTENT)
def follow(user_id: str, followee_id: str, session: SessionDep) -> Response:
    """Idempotent: following someone you already follow is a no-op."""
    if user_id == followee_id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "users cannot follow themselves")
    session.execute(
        insert(Follow).values(follower_id=user_id, followee_id=followee_id).on_conflict_do_nothing()
    )
    session.commit()
    log.info("follow follower_id=%s followee_id=%s", user_id, followee_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.delete("/users/{user_id}/following/{followee_id}", status_code=status.HTTP_204_NO_CONTENT)
def unfollow(user_id: str, followee_id: str, session: SessionDep) -> Response:
    session.execute(delete(Follow).where(Follow.follower_id == user_id, Follow.followee_id == followee_id))
    session.commit()
    log.info("unfollow follower_id=%s followee_id=%s", user_id, followee_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- Feed -------------------------------------------------------------------

def _encode_cursor(created_at: datetime, post_id: uuid.UUID) -> str:
    return base64.urlsafe_b64encode(f"{created_at.isoformat()}|{post_id}".encode()).decode()


def _decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    try:
        ts, pid = base64.urlsafe_b64decode(cursor.encode()).decode().split("|")
        return datetime.fromisoformat(ts), uuid.UUID(pid)
    except ValueError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid cursor") from None


@app.get("/users/{user_id}/feed")
def get_feed(
    user_id: str,
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: str | None = None,
) -> FeedPage:
    """Newest first. Keyset pagination: stable even while new posts arrive, unlike OFFSET."""
    query = (
        select(Post, FeedItem.created_at)
        .join(FeedItem, FeedItem.post_id == Post.id)
        .where(FeedItem.user_id == user_id)
        .order_by(FeedItem.created_at.desc(), FeedItem.post_id.desc())
        .limit(limit + 1)  # fetch one extra to know whether there's a next page
    )
    if cursor:
        ts, pid = _decode_cursor(cursor)
        query = query.where(tuple_(FeedItem.created_at, FeedItem.post_id) < tuple_(ts, pid))

    rows = session.execute(query).all()
    page = rows[:limit]
    next_cursor = None
    if len(rows) > limit:
        last_post, last_ts = page[-1]
        next_cursor = _encode_cursor(last_ts, last_post.id)
    return FeedPage(items=[PostOut.model_validate(p) for p, _ in page], next_cursor=next_cursor)
