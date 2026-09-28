"""
Fanout worker: consumes PostCreated events and writes the post into every
follower's feed in Postgres.

Run: uv run python -m feed_fanout.worker   (start several to scale out)

Failure handling:
  - Permanent failures (malformed event, unknown post) go to the dead-letter topic.
  - Transient failures (database unavailable) pause consumption and retry the same
    event with backoff until it succeeds. They're systemic: no other event could
    succeed either, so skipping ahead would only spread the damage.
  - Unexpected errors (bugs) are retried a few times, then dead-lettered.
The offset is committed only once the event is fanned out or safely dead-lettered.
"""

import enum
import logging
import time

from confluent_kafka import Consumer, Message, TopicPartition
from pydantic import ValidationError
from sqlalchemy import literal, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.config import (
    FANOUT_GROUP_ID,
    KAFKA_BOOTSTRAP_SERVERS,
    POST_CREATED_DLT_TOPIC,
    POST_CREATED_TOPIC,
)
from feed_fanout.db import SessionLocal
from feed_fanout.events import PostCreated
from feed_fanout.kafka import DeadLetterPublisher
from feed_fanout.models import FeedItem, Follow
from feed_fanout.retry import RetryPolicy, is_transient

log = logging.getLogger("feed_fanout.worker")

FOREIGN_KEY_VIOLATION = "23503"


class Outcome(enum.Enum):
    FANNED_OUT = "fanned_out"
    DEAD_LETTERED = "dead_lettered"


class PermanentError(Exception):
    """Processing this event can never succeed; retrying is pointless."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason


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


def handle(value: bytes | None, session_factory: sessionmaker[Session], where: str = "") -> int:
    """Process one event's payload. Returns feed rows added; raises PermanentError if it never can."""
    try:
        event = PostCreated.model_validate_json(value or b"")
    except ValidationError as e:
        raise PermanentError("malformed", str(e)) from e
    try:
        added = fan_out(session_factory, event)
    except IntegrityError as e:
        if getattr(e.orig, "sqlstate", None) == FOREIGN_KEY_VIOLATION:
            raise PermanentError("unknown_post", f"post {event.post_id} does not exist") from e
        raise
    log.info("fanned out post_id=%s author_id=%s feeds_added=%d %s",
             event.post_id, event.author_id, added, where)
    return added


class FanoutWorker:
    def __init__(
        self,
        consumer: Consumer,
        session_factory: sessionmaker[Session],
        dead_letters: DeadLetterPublisher,
        retry: RetryPolicy = RetryPolicy(),
    ) -> None:
        self.consumer = consumer
        self.session_factory = session_factory
        self.dead_letters = dead_letters
        self.retry = retry

    def process(self, msg: Message) -> Outcome:
        """Fan out or dead-letter one message, then commit its offset."""
        where = f"partition={msg.partition()} offset={msg.offset()}"
        attempt = 0
        while True:
            attempt += 1
            try:
                handle(msg.value(), self.session_factory, where)
                outcome = Outcome.FANNED_OUT
                break
            except PermanentError as e:
                self.dead_letters.send(msg, e.reason, str(e))
                outcome = Outcome.DEAD_LETTERED
                break
            except Exception as e:
                if not is_transient(e) and attempt >= self.retry.max_unexpected_attempts:
                    log.exception("giving up after %d attempts %s", attempt, where)
                    self.dead_letters.send(msg, "unexpected", repr(e))
                    outcome = Outcome.DEAD_LETTERED
                    break
                delay = self.retry.delay(attempt)
                log.warning("attempt %d failed (%s), retrying in %.1fs %s",
                            attempt, "transient" if is_transient(e) else "unexpected", delay, where)
                self._wait(delay)

        # Only now is the event fanned out or safely dead-lettered.
        self.consumer.commit(message=msg, asynchronous=False)
        return outcome

    def _wait(self, seconds: float) -> None:
        """Back off without being kicked out of the consumer group.

        Sleeping for long would exceed max.poll.interval.ms and trigger a rebalance, so pause
        every partition and keep polling instead. poll() keeps the membership alive.
        """
        assignment = self.consumer.assignment()
        self.consumer.pause(assignment)
        try:
            deadline = time.monotonic() + seconds
            while (remaining := deadline - time.monotonic()) > 0:
                msg = self.consumer.poll(min(remaining, 1.0))
                if msg is not None and not msg.error():
                    # Already fetched before the pause. Rewind so it's re-read after resuming,
                    # instead of silently skipped.
                    self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
        finally:
            self.consumer.resume(assignment)

    def run(self, topic: str) -> None:
        self.consumer.subscribe([topic])
        log.info("consuming topic=%s", topic)
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log.error("consumer error: %s", msg.error())
                    continue
                self.process(msg)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()  # leave the group so partitions are reassigned right away
            self.dead_letters.close()
            log.info("closed")


def make_consumer(bootstrap_servers: str, group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        # At-least-once: offsets are committed explicitly in process(), after handling.
        "enable.auto.commit": False,
    })


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log.info("group=%s dead_letter_topic=%s", FANOUT_GROUP_ID, POST_CREATED_DLT_TOPIC)
    worker = FanoutWorker(
        make_consumer(KAFKA_BOOTSTRAP_SERVERS, FANOUT_GROUP_ID),
        SessionLocal,
        DeadLetterPublisher(KAFKA_BOOTSTRAP_SERVERS, POST_CREATED_DLT_TOPIC),
    )
    worker.run(POST_CREATED_TOPIC)


if __name__ == "__main__":
    main()
