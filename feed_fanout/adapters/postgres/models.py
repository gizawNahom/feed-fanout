import uuid
from datetime import UTC, datetime

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Identity, Index, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

USER_ID = String(64)  # users are identified by handle; there is no users table yet


class Base(DeclarativeBase):
    pass


def utcnow() -> datetime:
    return datetime.now(UTC)


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    author_id: Mapped[str] = mapped_column(USER_ID)
    text: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Follow(Base):
    """follower_id follows followee_id."""

    __tablename__ = "follows"

    follower_id: Mapped[str] = mapped_column(USER_ID, primary_key=True)
    followee_id: Mapped[str] = mapped_column(USER_ID, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    __table_args__ = (
        CheckConstraint("follower_id <> followee_id", name="ck_follows_not_self"),
        # Fanout asks "who follows this author?" -> needs followee_id first.
        Index("ix_follows_followee_id", "followee_id"),
    )


class FeedItem(Base):
    """One post in one user's precomputed feed. Written by the fanout worker."""

    __tablename__ = "feed_items"

    # (user_id, post_id) is the primary key, so fanning out the same post twice is a no-op.
    user_id: Mapped[str] = mapped_column(USER_ID, primary_key=True)
    post_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("posts.id", ondelete="CASCADE"), primary_key=True)
    # Copied from the post so the feed can be ordered without joining posts.
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        # Serves "newest N posts in this user's feed", including keyset pagination.
        Index("ix_feed_items_user_id_created_at", "user_id", "created_at", "post_id"),
    )


class OutboxEvent(Base):
    """An event waiting to be published to Kafka (transactional outbox).

    Written in the same transaction as the change it describes, so the event exists
    if and only if the change was committed. The relay publishes rows and deletes them.
    """

    __tablename__ = "outbox"

    # Monotonic id: the relay publishes in insertion order.
    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    topic: Mapped[str] = mapped_column(String(255))
    key: Mapped[str | None] = mapped_column(String(255))
    payload: Mapped[str] = mapped_column(Text)  # serialized event (JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class PostingBucket(Base):
    """A user's posting token bucket: the time it's full again (see domain/posting_limit.py).

    One small row per user who has posted. A row whose full_at has passed means the same as no
    row (a full bucket), so old rows are harmless and could be purged at any time.
    """

    __tablename__ = "posting_buckets"

    user_id: Mapped[str] = mapped_column(USER_ID, primary_key=True)
    full_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))  # NULL: never posted
