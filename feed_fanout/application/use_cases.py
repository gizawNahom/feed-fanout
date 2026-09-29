"""
Use cases: everything the system does, one class each. They apply the domain rules and
decide what gets saved, through the ports; they know nothing about HTTP, SQL or Kafka.
"""

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from feed_fanout.application.ports import Clock, UnitOfWork
from feed_fanout.domain.feed import FeedPage, FeedPosition
from feed_fanout.domain.model import DEFAULT_PAGE_SIZE, Post, check_can_follow, check_page_size
from feed_fanout.domain.posting_limit import DEFAULT_POSTING_LIMIT, PostingLimit

UnitOfWorkFactory = Callable[[], UnitOfWork]


@dataclass(frozen=True)
class CreatePost:
    """Save a post and, atomically with it, record that it was created (for fanout).

    Refused with TooManyPosts if the author is over their posting limit. An invalid post is
    refused before that, so it doesn't count.
    """

    unit_of_work: UnitOfWorkFactory
    clock: Clock
    limit: PostingLimit = DEFAULT_POSTING_LIMIT

    def __call__(self, author_id: str, text: str) -> Post:
        now = self.clock.now()
        post = Post.create(author_id, text, now=now)
        with self.unit_of_work() as uow:
            buckets = uow.posting_buckets
            buckets.set_full_at(author_id, self.limit.take(buckets.full_at(author_id), now))
            uow.posts.add(post)
            uow.outbox.post_created(post)
            uow.commit()
        return post


@dataclass(frozen=True)
class Follow:
    unit_of_work: UnitOfWorkFactory

    def __call__(self, follower_id: str, followee_id: str) -> None:
        check_can_follow(follower_id, followee_id)
        with self.unit_of_work() as uow:
            uow.follows.add(follower_id, followee_id)
            uow.commit()


@dataclass(frozen=True)
class Unfollow:
    """Stops new posts from arriving. Posts already in the feed stay."""

    unit_of_work: UnitOfWorkFactory

    def __call__(self, follower_id: str, followee_id: str) -> None:
        with self.unit_of_work() as uow:
            uow.follows.remove(follower_id, followee_id)
            uow.commit()


@dataclass(frozen=True)
class ReadFeed:
    unit_of_work: UnitOfWorkFactory

    def __call__(self, user_id: str, limit: int = DEFAULT_PAGE_SIZE, cursor: str | None = None) -> FeedPage:
        check_page_size(limit)
        before = FeedPosition.from_cursor(cursor) if cursor is not None else None
        with self.unit_of_work() as uow:
            posts = uow.feeds.newest(user_id, limit + 1, before)  # one extra: is there a next page?
        page = posts[:limit]
        next_cursor = FeedPosition.of(page[-1]).to_cursor() if len(posts) > limit else None
        return FeedPage(posts=page, next_cursor=next_cursor)


@dataclass(frozen=True)
class FanOutPost:
    """Put a new post into its author's followers' feeds (fanout-on-write).

    Safe to repeat: events can be delivered more than once, and doing it twice adds nothing.
    """

    unit_of_work: UnitOfWorkFactory

    def __call__(self, post_id: uuid.UUID, author_id: str, created_at: datetime) -> int:
        with self.unit_of_work() as uow:
            added = uow.feeds.add_to_followers(post_id, author_id, created_at)
            uow.commit()
        return added
