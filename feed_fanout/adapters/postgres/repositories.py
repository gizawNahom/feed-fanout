"""The ports, implemented with Postgres (through SQLAlchemy)."""

import uuid
from datetime import datetime
from types import TracebackType
from typing import Self

from sqlalchemy import delete, literal, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.adapters.kafka.events import PostCreated
from feed_fanout.adapters.postgres import models
from feed_fanout.application.ports import UnknownPost
from feed_fanout.domain.feed import FeedPosition
from feed_fanout.domain.model import Post

FOREIGN_KEY_VIOLATION = "23503"


class PostgresPosts:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, post: Post) -> None:
        self.session.add(models.Post(id=post.id, author_id=post.author_id, text=post.text, created_at=post.created_at))


class PostgresFollows:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, follower_id: str, followee_id: str) -> None:
        self.session.execute(
            insert(models.Follow).values(follower_id=follower_id, followee_id=followee_id).on_conflict_do_nothing()
        )

    def remove(self, follower_id: str, followee_id: str) -> None:
        self.session.execute(
            delete(models.Follow).where(
                models.Follow.follower_id == follower_id, models.Follow.followee_id == followee_id
            )
        )


class PostgresFeeds:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add_to_followers(self, post_id: uuid.UUID, author_id: str, created_at: datetime) -> int:
        # One INSERT ... SELECT: Postgres reads the followers and writes the feed rows itself,
        # so nothing is loaded into Python, however many followers there are. ON CONFLICT DO
        # NOTHING makes it idempotent; RETURNING counts only rows actually inserted.
        followers = select(models.Follow.follower_id, literal(post_id), literal(created_at)).where(
            models.Follow.followee_id == author_id
        )
        stmt = (
            insert(models.FeedItem)
            .from_select(["user_id", "post_id", "created_at"], followers)
            .on_conflict_do_nothing()
            .returning(models.FeedItem.user_id)
        )
        try:
            return len(self.session.execute(stmt).all())
        except IntegrityError as e:
            if getattr(e.orig, "sqlstate", None) == FOREIGN_KEY_VIOLATION:
                raise UnknownPost(f"post {post_id} does not exist") from e
            raise

    def newest(self, user_id: str, limit: int, before: FeedPosition | None) -> list[Post]:
        item = models.FeedItem
        query = (
            select(models.Post)
            .join(item, item.post_id == models.Post.id)
            .where(item.user_id == user_id)
            .order_by(item.created_at.desc(), item.post_id.desc())
            .limit(limit)
        )
        if before is not None:
            # Keyset pagination: served by the (user_id, created_at, post_id) index.
            query = query.where(tuple_(item.created_at, item.post_id) < tuple_(before.created_at, before.post_id))
        return [
            Post(id=row.id, author_id=row.author_id, text=row.text, created_at=row.created_at)
            for row in self.session.scalars(query)
        ]


class PostgresOutbox:
    def __init__(self, session: Session, topic: str) -> None:
        self.session = session
        self.topic = topic

    def post_created(self, post: Post) -> None:
        event = PostCreated(post_id=post.id, author_id=post.author_id, created_at=post.created_at)
        # key = author: all of an author's events land on the same Kafka partition, in order.
        self.session.add(models.OutboxEvent(topic=self.topic, key=post.author_id, payload=event.model_dump_json()))


class PostgresPostingBuckets:
    def __init__(self, session: Session) -> None:
        self.session = session

    def full_at(self, user_id: str) -> datetime | None:
        # Make sure there's a row to lock, then lock it until the transaction ends. A concurrent
        # post by the same user waits here, then reads what this one wrote. (Even for a user's
        # very first posts: the second insert waits on the first's, then does nothing.)
        self.session.execute(insert(models.PostingBucket).values(user_id=user_id).on_conflict_do_nothing())
        bucket = models.PostingBucket
        return self.session.scalars(select(bucket.full_at).where(bucket.user_id == user_id).with_for_update()).one()

    def set_full_at(self, user_id: str, full_at: datetime) -> None:
        bucket = models.PostingBucket
        self.session.execute(update(bucket).where(bucket.user_id == user_id).values(full_at=full_at))


class PostgresUnitOfWork:
    """One database transaction: everything done through it commits together, or not at all."""

    def __init__(self, session_factory: sessionmaker[Session], post_created_topic: str) -> None:
        self.session_factory = session_factory
        self.post_created_topic = post_created_topic

    def __enter__(self) -> Self:
        self.session = self.session_factory()
        self.posts = PostgresPosts(self.session)
        self.follows = PostgresFollows(self.session)
        self.feeds = PostgresFeeds(self.session)
        self.outbox = PostgresOutbox(self.session, self.post_created_topic)
        self.posting_buckets = PostgresPostingBuckets(self.session)
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.session.rollback()  # a no-op after commit(); otherwise discards partial work
        self.session.close()

    def commit(self) -> None:
        self.session.commit()
