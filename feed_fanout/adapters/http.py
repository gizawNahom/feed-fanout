"""
HTTP API: create posts, follow users, read feeds.

Run: uv run uvicorn feed_fanout.adapters.http:app --reload
Docs: http://localhost:8000/docs
"""

import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import datetime
from typing import Annotated

from fastapi import Depends, FastAPI, Query, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from feed_fanout import metrics
from feed_fanout.adapters.clock import SystemClock
from feed_fanout.adapters.postgres.db import SessionLocal
from feed_fanout.adapters.postgres.repositories import PostgresUnitOfWork
from feed_fanout.application.ports import Clock
from feed_fanout.application.use_cases import CreatePost, Follow, ReadFeed, Unfollow, UnitOfWorkFactory
from feed_fanout.config import settings
from feed_fanout.domain.model import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    MAX_POST_LENGTH,
    MAX_USER_ID_LENGTH,
    CannotFollowYourself,
    InvalidCursor,
    Rejected,
)
from feed_fanout.logs import configure_logging, get_logger

configure_logging(settings.log_level, settings.log_format)
log = get_logger("feed_fanout.adapters.http")


app = FastAPI(title="feed-fanout")


@app.middleware("http")
async def record_metrics(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
    """Count and time every request, labelled by route *template* (/users/{user_id}/feed),
    never the raw path: /users/bob/feed would create one time series per user."""
    start = time.perf_counter()
    status_code = 500  # if the handler raises, the client gets a 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        template = route.path if isinstance(route, APIRoute) else "unmatched"
        metrics.HTTP_DURATION.labels(request.method, template).observe(time.perf_counter() - start)
        metrics.HTTP_REQUESTS.labels(request.method, template, str(status_code)).inc()


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# --- Wiring: which adapters the use cases get. Tests override these. ------------


def get_session() -> Iterator[Session]:
    """Only for the readiness check; everything else goes through the use cases."""
    with SessionLocal() as session:
        yield session


def get_unit_of_work() -> UnitOfWorkFactory:
    return lambda: PostgresUnitOfWork(SessionLocal, settings.post_created_topic)


def get_clock() -> Clock:
    return SystemClock()


SessionDep = Annotated[Session, Depends(get_session)]
UnitOfWorkDep = Annotated[UnitOfWorkFactory, Depends(get_unit_of_work)]
ClockDep = Annotated[Clock, Depends(get_clock)]


@app.exception_handler(Rejected)
def rejected(request: Request, e: Rejected) -> JSONResponse:
    """A broken domain rule becomes a 4xx: 400 for a bad request, 422 for invalid input."""
    code = 400 if isinstance(e, CannotFollowYourself | InvalidCursor) else 422
    return JSONResponse(status_code=code, content={"detail": str(e)})


# --- Schemas: the HTTP representation. Limits come from the domain, so they can't drift. ---

UserId = Annotated[str, Field(min_length=1, max_length=MAX_USER_ID_LENGTH)]


class CreatePostBody(BaseModel):
    author_id: UserId
    text: str = Field(min_length=1, max_length=MAX_POST_LENGTH)


class PostOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    author_id: str
    text: str
    created_at: datetime


class FeedPageOut(BaseModel):
    items: list[PostOut]
    next_cursor: str | None  # pass as ?cursor= to get the next (older) page


# --- Health -----------------------------------------------------------------


@app.get("/health/live", tags=["health"])
def live() -> dict[str, str]:
    """The process is up and serving requests. Checks no dependencies on purpose: restarting
    the API wouldn't fix a database outage, and restarting every instance at once would hurt."""
    return {"status": "ok"}


@app.get("/health/ready", tags=["health"], responses={503: {"description": "A dependency is unavailable"}})
def ready(session: SessionDep) -> JSONResponse:
    """Can serve requests right now: the database answers. If not, stop sending traffic here."""
    try:
        session.execute(text("SELECT 1"))
    except SQLAlchemyError as e:
        log.warning("not ready: database unavailable", error=e.__class__.__name__)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unavailable", "checks": {"database": str(getattr(e, "orig", None) or e).strip()}},
        )
    return JSONResponse({"status": "ok", "checks": {"database": "ok"}})


# --- Posts ------------------------------------------------------------------


@app.post("/posts", status_code=status.HTTP_201_CREATED)
def create_post(body: CreatePostBody, unit_of_work: UnitOfWorkDep, clock: ClockDep) -> PostOut:
    post = CreatePost(unit_of_work, clock)(body.author_id, body.text)
    log.info("post created", post_id=str(post.id), author_id=post.author_id)
    return PostOut.model_validate(post)


# --- Follows ----------------------------------------------------------------


@app.put("/users/{user_id}/following/{followee_id}", status_code=status.HTTP_204_NO_CONTENT)
def follow(user_id: str, followee_id: str, unit_of_work: UnitOfWorkDep) -> Response:
    """Idempotent: following someone you already follow is a no-op."""
    Follow(unit_of_work)(user_id, followee_id)
    log.info("followed", follower_id=user_id, followee_id=followee_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.delete("/users/{user_id}/following/{followee_id}", status_code=status.HTTP_204_NO_CONTENT)
def unfollow(user_id: str, followee_id: str, unit_of_work: UnitOfWorkDep) -> Response:
    Unfollow(unit_of_work)(user_id, followee_id)
    log.info("unfollowed", follower_id=user_id, followee_id=followee_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- Feed -------------------------------------------------------------------


@app.get("/users/{user_id}/feed")
def get_feed(
    user_id: str,
    unit_of_work: UnitOfWorkDep,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    cursor: str | None = None,
) -> FeedPageOut:
    """Newest first. Keyset pagination: stable even while new posts arrive, unlike OFFSET."""
    page = ReadFeed(unit_of_work)(user_id, limit, cursor)
    return FeedPageOut(items=[PostOut.model_validate(p) for p in page.posts], next_cursor=page.next_cursor)
