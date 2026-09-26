"""
Fanout worker: consumes PostCreated events and writes the post into every
follower's feed in Postgres.

Run: uv run python -m feed_fanout.worker   (start several to scale out)
"""

import logging

from confluent_kafka import Consumer, Message
from pydantic import ValidationError
from sqlalchemy import literal, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from feed_fanout.config import FANOUT_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS, POST_CREATED_TOPIC
from feed_fanout.db import SessionLocal
from feed_fanout.events import PostCreated
from feed_fanout.models import FeedItem, Follow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("feed_fanout.worker")


def fan_out(event: PostCreated) -> int:
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
    with SessionLocal.begin() as session:  # commits on success, rolls back on error
        return len(session.execute(stmt).all())


def handle(msg: Message) -> None:
    where = f"partition={msg.partition()} offset={msg.offset()}"
    try:
        event = PostCreated.model_validate_json(msg.value())
    except ValidationError as e:
        # Unparseable events would crash the worker forever. Skip for now; dead-letter topic in v4.
        log.warning("skipped malformed event %s value=%r errors=%d", where, msg.value(), e.error_count())
        return
    try:
        added = fan_out(event)
    except IntegrityError:
        # The post doesn't exist (e.g. deleted). Retrying won't help, so skip.
        log.warning("skipped event for unknown post post_id=%s %s", event.post_id, where)
        return
    log.info("fanned out post_id=%s author_id=%s feeds_added=%d %s",
             event.post_id, event.author_id, added, where)
    # Any other error (e.g. DB down) propagates: the worker stops WITHOUT committing
    # the offset, so the event is re-delivered when it restarts.


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": FANOUT_GROUP_ID,
        "auto.offset.reset": "earliest",
        # At-least-once: commit only after fanout succeeds (below).
        "enable.auto.commit": False,
    })
    consumer.subscribe([POST_CREATED_TOPIC])
    log.info("consuming topic=%s group=%s", POST_CREATED_TOPIC, FANOUT_GROUP_ID)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue
            handle(msg)
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()  # leave the group so partitions are reassigned right away
        log.info("closed")


if __name__ == "__main__":
    main()
