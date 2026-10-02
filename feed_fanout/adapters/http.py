"""
HTTP API: create posts, follow users, read feeds.

Run: uv run uvicorn feed_fanout.adapters.http:app --reload
Docs: http://localhost:8000/docs
"""

import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import datetime
from functools import lru_cache
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from feed_fanout import metrics
from feed_fanout.adapters.auth import InvalidToken, TokenVerifier, build_verifier
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
    CannotFollowYourself,
    InvalidCursor,
    Rejected,
)
from feed_fanout.domain.posting_limit import PostingLimit, TooManyPosts
from feed_fanout.logs import configure_logging, get_logger

configure_logging(settings.log_level, settings.log_format)
log = get_logger("feed_fanout.adapters.http")


@lru_cache
def token_verifier() -> TokenVerifier:
    return build_verifier(settings)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Fail at startup, not on the first request, if tokens can't be verified.
    override = app.dependency_overrides.get(get_token_verifier)
    override() if override else token_verifier()
    yield


app = FastAPI(title="feed-fanout", lifespan=lifespan)


# --- Middleware ----------------------------------------------------------------
#
# Plain ASGI middleware, not @app.middleware("http"). That decorator wraps each function in
# Starlette's BaseHTTPMiddleware, which starts a task group and an in-memory stream for every
# request just to pass the response through. Profiled under load, that machinery (anyio task
# groups, streams, locks) was a large share of the API's CPU, more than our own code.


class RecordMetrics:
    """Count and time every request, labelled by route *template* (/users/{user_id}/feed),
    never the raw path: /users/bob/feed would create one time series per user."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        start = time.perf_counter()
        status_code = 500  # if the app raises before responding, the client gets a 500

        async def send_and_note_status(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_and_note_status)
        finally:
            route = scope.get("route")  # set by the router once it has matched the request
            template = route.path if isinstance(route, APIRoute) else "unmatched"
            metrics.HTTP_DURATION.labels(scope["method"], template).observe(time.perf_counter() - start)
            metrics.HTTP_REQUESTS.labels(scope["method"], template, str(status_code)).inc()


class SecurityHeaders:
    """Defense in depth: never let a browser guess that a response (say, a post's text in JSON) is
    HTML it should render. Every response, errors included."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)["X-Content-Type-Options"] = "nosniff"
            await send(message)

        await self.app(scope, receive, send_with_headers)


app.add_middleware(RecordMetrics)
app.add_middleware(SecurityHeaders)  # added last, so it's the outer one


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# --- Wiring: which adapters the use cases get. Tests override these. ------------
#
# `async def` on purpose, though nothing here awaits: FastAPI runs every plain `def` dependency
# in a worker thread, a handoff per dependency per request. These only return an object, so
# they run on the event loop instead. What does block stays `def` and keeps its thread: the
# endpoints (they wait on the database), get_session, and current_user (below).


def get_session() -> Iterator[Session]:
    """Only for the readiness check; everything else goes through the use cases."""
    with SessionLocal() as session:
        yield session


async def get_unit_of_work() -> UnitOfWorkFactory:
    return lambda: PostgresUnitOfWork(SessionLocal, settings.post_created_topic)


async def get_clock() -> Clock:
    return SystemClock()


async def get_posting_limit() -> PostingLimit:
    return settings.posting_limit


async def get_token_verifier() -> TokenVerifier:
    return token_verifier()


SessionDep = Annotated[Session, Depends(get_session)]
UnitOfWorkDep = Annotated[UnitOfWorkFactory, Depends(get_unit_of_work)]
ClockDep = Annotated[Clock, Depends(get_clock)]
PostingLimitDep = Annotated[PostingLimit, Depends(get_posting_limit)]

_bearer = HTTPBearer(auto_error=False, description="An access token from the identity provider")


def current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    verifier: Annotated[TokenVerifier, Depends(get_token_verifier)],
) -> str:
    """Who is making the request, from their verified access token. The only source of identity:
    no user id in bodies or paths, so nobody can even ask to act as someone else.

    A plain `def`, so it runs in a worker thread: verifying a token with a key we haven't seen
    fetches the provider's keys (JWKS) over the network, and on the event loop that would freeze
    every request until it returned."""
    if credentials is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "missing bearer token", headers={"WWW-Authenticate": "Bearer"}
        )
    try:
        return verifier.user_id(credentials.credentials)
    except InvalidToken as e:
        log.info("rejected access token", reason=str(e))  # the reason stays in our logs, not the response
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "invalid token", headers={"WWW-Authenticate": 'Bearer error="invalid_token"'}
        ) from None


CurrentUser = Annotated[str, Depends(current_user)]


@app.exception_handler(TooManyPosts)
def too_many_posts(request: Request, e: TooManyPosts) -> JSONResponse:
    log.info("post refused: posting limit", retry_after=e.retry_after_seconds)
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={"detail": str(e)},
        headers={"Retry-After": str(e.retry_after_seconds)},
    )


@app.exception_handler(Rejected)
def rejected(request: Request, e: Rejected) -> JSONResponse:
    """A broken domain rule becomes a 4xx: 400 for a bad request, 422 for invalid input."""
    code = 400 if isinstance(e, CannotFollowYourself | InvalidCursor) else 422
    return JSONResponse(status_code=code, content={"detail": str(e)})


# --- Schemas: the HTTP representation. Limits come from the domain, so they can't drift. ---


class CreatePostBody(BaseModel):
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


@app.post(
    "/posts",
    status_code=status.HTTP_201_CREATED,
    responses={429: {"description": "Posting too fast: try again after Retry-After seconds"}},
)
def create_post(
    body: CreatePostBody, user: CurrentUser, unit_of_work: UnitOfWorkDep, clock: ClockDep, limit: PostingLimitDep
) -> PostOut:
    """Post as the signed-in user. Each user can post a burst of POSTS_BURST, then POSTS_PER_MINUTE."""
    post = CreatePost(unit_of_work, clock, limit)(user, body.text)
    log.info("post created", post_id=str(post.id), author_id=post.author_id)
    return PostOut.model_validate(post)


# --- Follows ----------------------------------------------------------------


@app.put("/me/following/{followee_id}", status_code=status.HTTP_204_NO_CONTENT)
def follow(followee_id: str, user_id: CurrentUser, unit_of_work: UnitOfWorkDep) -> Response:
    """Idempotent: following someone you already follow is a no-op."""
    Follow(unit_of_work)(user_id, followee_id)
    log.info("followed", follower_id=user_id, followee_id=followee_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.delete("/me/following/{followee_id}", status_code=status.HTTP_204_NO_CONTENT)
def unfollow(followee_id: str, user_id: CurrentUser, unit_of_work: UnitOfWorkDep) -> Response:
    Unfollow(unit_of_work)(user_id, followee_id)
    log.info("unfollowed", follower_id=user_id, followee_id=followee_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- Feed -------------------------------------------------------------------


@app.get("/me/feed")
def get_feed(
    user_id: CurrentUser,
    unit_of_work: UnitOfWorkDep,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    cursor: str | None = None,
) -> FeedPageOut:
    """The signed-in user's feed (feeds are private). Newest first; keyset pagination is
    stable even while new posts arrive, unlike OFFSET."""
    page = ReadFeed(unit_of_work)(user_id, limit, cursor)
    return FeedPageOut(items=[PostOut.model_validate(p) for p in page.posts], next_cursor=page.next_cursor)
