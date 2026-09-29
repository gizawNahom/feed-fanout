"""
In-memory implementations of the ports, for fast tests (no Docker, no I/O).

They must behave like the Postgres adapters in every way the use cases can observe:
transactions, idempotency, ordering, and even the unknown-post quirk. The FeedRepository
contract tests run the same checks against both, so if this fake drifts from Postgres,
they fail.
"""

import copy
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Self

from feed_fanout.application.ports import UnknownPost
from feed_fanout.domain.feed import FeedPosition
from feed_fanout.domain.model import Post


@dataclass(frozen=True)
class OutboxEntry:
    post_id: uuid.UUID
    author_id: str
    created_at: datetime


@dataclass
class InMemoryState:
    """What the database would hold. Unit-of-work changes are applied to it on commit."""

    posts: dict[uuid.UUID, Post] = field(default_factory=dict)
    follows: set[tuple[str, str]] = field(default_factory=set)  # (follower, followee)
    feeds: dict[str, dict[uuid.UUID, FeedPosition]] = field(default_factory=dict)  # user -> post_id -> position
    outbox: list[OutboxEntry] = field(default_factory=list)
    posting_buckets: dict[str, datetime] = field(default_factory=dict)  # user -> full_at


class InMemoryPosts:
    def __init__(self, state: InMemoryState) -> None:
        self.state = state

    def add(self, post: Post) -> None:
        self.state.posts[post.id] = post


class InMemoryFollows:
    def __init__(self, state: InMemoryState) -> None:
        self.state = state

    def add(self, follower_id: str, followee_id: str) -> None:
        self.state.follows.add((follower_id, followee_id))

    def remove(self, follower_id: str, followee_id: str) -> None:
        self.state.follows.discard((follower_id, followee_id))


class InMemoryFeeds:
    def __init__(self, state: InMemoryState) -> None:
        self.state = state

    def add_to_followers(self, post_id: uuid.UUID, author_id: str, created_at: datetime) -> int:
        followers = sorted(f for f, followee in self.state.follows if followee == author_id)
        # Like the foreign key in Postgres: only checked when there's a row to insert.
        if followers and post_id not in self.state.posts:
            raise UnknownPost(f"post {post_id} does not exist")
        added = 0
        for follower in followers:
            feed = self.state.feeds.setdefault(follower, {})
            if post_id not in feed:
                feed[post_id] = FeedPosition(created_at, post_id)
                added += 1
        return added

    def newest(self, user_id: str, limit: int, before: FeedPosition | None) -> list[Post]:
        positions = sorted(self.state.feeds.get(user_id, {}).values(), reverse=True)
        if before is not None:
            positions = [p for p in positions if p < before]
        return [self.state.posts[p.post_id] for p in positions[:limit]]


class InMemoryOutbox:
    def __init__(self, state: InMemoryState) -> None:
        self.state = state

    def post_created(self, post: Post) -> None:
        self.state.outbox.append(OutboxEntry(post.id, post.author_id, post.created_at))


class InMemoryPostingBuckets:
    """No locking needed: one thread, and each unit of work works on its own copy."""

    def __init__(self, state: InMemoryState) -> None:
        self.state = state

    def full_at(self, user_id: str) -> datetime | None:
        return self.state.posting_buckets.get(user_id)

    def set_full_at(self, user_id: str, full_at: datetime) -> None:
        self.state.posting_buckets[user_id] = full_at


class InMemoryUnitOfWork:
    """Works on a copy of the state; commit() makes the copy the state. Like a transaction."""

    def __init__(self, db: "InMemoryDatabase") -> None:
        self.db = db

    def __enter__(self) -> Self:
        self.working = copy.deepcopy(self.db.state)
        self.posts = InMemoryPosts(self.working)
        self.follows = InMemoryFollows(self.working)
        self.feeds = InMemoryFeeds(self.working)
        self.outbox = InMemoryOutbox(self.working)
        self.posting_buckets = InMemoryPostingBuckets(self.working)
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        pass  # uncommitted changes are simply dropped with the copy

    def commit(self) -> None:
        self.db.state = self.working


class InMemoryDatabase:
    def __init__(self) -> None:
        self.state = InMemoryState()

    def unit_of_work(self) -> InMemoryUnitOfWork:
        return InMemoryUnitOfWork(self)


class TickingClock:
    """Each call is 1ms after the last: posts get distinct, increasing timestamps, as they
    would in real time. (In memory, several posts can be created within one microsecond.)"""

    def __init__(self, start: datetime = datetime(2026, 1, 1, tzinfo=UTC)) -> None:
        self.current = start

    def now(self) -> datetime:
        self.current += timedelta(milliseconds=1)
        return self.current

    def advance(self, delta: timedelta) -> None:
        self.current += delta
