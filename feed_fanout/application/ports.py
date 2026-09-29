"""
Ports: what the use cases need from the outside world, as interfaces. Adapters implement
them: Postgres in production, in memory for fast tests. The use cases never know which.
"""

import uuid
from datetime import datetime
from types import TracebackType
from typing import Protocol, Self

from feed_fanout.domain.feed import FeedPosition
from feed_fanout.domain.model import Post


class UnknownPost(Exception):
    """An event refers to a post that doesn't exist."""


class PostRepository(Protocol):
    def add(self, post: Post) -> None: ...


class FollowRepository(Protocol):
    def add(self, follower_id: str, followee_id: str) -> None:
        """Idempotent: adding an existing follow changes nothing."""
        ...

    def remove(self, follower_id: str, followee_id: str) -> None:
        """Idempotent: removing a follow that doesn't exist changes nothing."""
        ...


class FeedRepository(Protocol):
    def add_to_followers(self, post_id: uuid.UUID, author_id: str, created_at: datetime) -> int:
        """Put the post into the feed of everyone who follows the author right now.

        Idempotent: doing it twice adds nothing the second time. Returns how many feed
        entries were added. Raises UnknownPost if the post doesn't exist (only noticed when
        there's at least one follower to add it for).
        """
        ...

    def newest(self, user_id: str, limit: int, before: FeedPosition | None) -> list[Post]:
        """Up to `limit` posts from the user's feed, newest first, strictly older than `before`."""
        ...


class Outbox(Protocol):
    def post_created(self, post: Post) -> None:
        """Record that the post was created, to be announced to the rest of the system.

        Recorded in the same unit of work as the post itself: both happen, or neither.
        """
        ...


class UnitOfWork(Protocol):
    """One atomic change: everything done through it is committed together, or not at all."""

    @property
    def posts(self) -> PostRepository: ...

    @property
    def follows(self) -> FollowRepository: ...

    @property
    def feeds(self) -> FeedRepository: ...

    @property
    def outbox(self) -> Outbox: ...

    def __enter__(self) -> Self: ...

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        """Roll back anything not committed."""
        ...

    def commit(self) -> None: ...


class Clock(Protocol):
    def now(self) -> datetime: ...
