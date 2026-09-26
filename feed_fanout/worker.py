"""
Fanout worker: consumes PostCreated events and writes the post into every
follower's feed in Postgres.

Run: uv run python -m feed_fanout.worker   (start several to scale out)
"""

import enum
import logging

from confluent_kafka import Consumer, Message
from pydantic import ValidationError
from sqlalchemy import literal, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.config import FANOUT_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS, POST_CREATED_TOPIC
from feed_fanout.db import SessionLocal
from feed_fanout.events import PostCreated
from feed_fanout.models import FeedItem, Follow

log = logging.getLogger("feed_fanout.worker")


class Outcome(enum.Enum):
    FANNED_OUT = "fanned_out"
    SKIPPED_MALFORMED = "skipped_malformed"
    SKIPPED_UNKNOWN_POST = "skipped_unknown_post"


def fan_out(session_factory: sessionmaker[Session], event: PostCreated) -> int:
    """Insert the post into each follower's feed. Returns the number of new feed rows.

    One INSERT ... SELECT: Postgres reads the followers and writes the feed rows itself,
    so nothing is loaded into Python. ON CONFLICT DO NOTHING makes it idempotent:
    re-processing the same event (at-least-once delivery) adds nothing.
    """
    followers = select(
        Follow.follower_id,
        literal(event.post_id),
        literal(event.created_at),
    ).where(Follow.followee_id == event.author_id)

    stmt = (
        insert(FeedItem)
        .from_select(["user_id", "post_id", "created_at"], followers)
        .on_conflict_do_nothing()
        .returning(FeedItem.user_id)  # rows actually inserted (conflicts return nothing)
    )
    with session_factory.begin() as session:  # commits on success, rolls back on error
        return len(session.execute(stmt).all())


def handle(value: bytes | None, session_factory: sessionmaker[Session], where: str = "") -> Outcome:
    """Process one event's payload."""
    try:
        event = PostCreated.model_validate_json(value or b"")
    except ValidationError as e:
        # Unparseable events would crash the worker forever. Skip for now; dead-letter topic in v4.
        log.warning("skipped malformed event %s value=%r errors=%d", where, value, e.error_count())
        return Outcome.SKIPPED_MALFORMED
    try:
        added = fan_out(session_factory, event)
    except IntegrityError:
        # The post doesn't exist (e.g. deleted). Retrying won't help, so skip.
        log.warning("skipped event for unknown post post_id=%s %s", event.post_id, where)
        return Outcome.SKIPPED_UNKNOWN_POST
    log.info("fanned out post_id=%s author_id=%s feeds_added=%d %s",
             event.post_id, event.author_id, added, where)
    return Outcome.FANNED_OUT
    # Any other error (e.g. DB down) propagates: the worker stops WITHOUT committing
    # the offset, so the event is re-delivered when it restarts.


def process(msg: Message, consumer: Consumer, session_factory: sessionmaker[Session]) -> Outcome:
    """Handle one message, then commit its offset. At-least-once: commit only after handling."""
    outcome = handle(msg.value(), session_factory, f"partition={msg.partition()} offset={msg.offset()}")
    consumer.commit(message=msg, asynchronous=False)
    return outcome


def make_consumer(bootstrap_servers: str, group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        # At-least-once: offsets are committed explicitly in process(), after handling.
        "enable.auto.commit": False,
    })


def run(consumer: Consumer, session_factory: sessionmaker[Session], topic: str) -> None:
    consumer.subscribe([topic])
    log.info("consuming topic=%s", topic)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue
            process(msg, consumer, session_factory)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()  # leave the group so partitions are reassigned right away
        log.info("closed")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log.info("group=%s", FANOUT_GROUP_ID)
    run(make_consumer(KAFKA_BOOTSTRAP_SERVERS, FANOUT_GROUP_ID), SessionLocal, POST_CREATED_TOPIC)


if __name__ == "__main__":
    main()
