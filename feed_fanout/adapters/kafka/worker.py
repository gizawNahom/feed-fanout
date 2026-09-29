"""
Fanout worker: consumes PostCreated events and writes the post into every
follower's feed in Postgres.

Run: uv run python -m feed_fanout.adapters.kafka.worker   (start several to scale out)

Failure handling:
  - Permanent failures (malformed event, unknown post) go to the dead-letter topic.
  - Transient failures (database unavailable) pause consumption and retry the same
    event with backoff until it succeeds. They're systemic: no other event could
    succeed either, so skipping ahead would only spread the damage.
  - Unexpected errors (bugs) are retried a few times, then dead-lettered.
The offset is committed only once the event is fanned out or safely dead-lettered.
"""

import enum
import threading
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
from pydantic import ValidationError
from structlog.contextvars import bound_contextvars

from feed_fanout import metrics
from feed_fanout.adapters.kafka.events import PostCreated
from feed_fanout.adapters.kafka.producers import DeadLetterPublisher
from feed_fanout.adapters.postgres.db import SessionLocal
from feed_fanout.adapters.postgres.repositories import PostgresUnitOfWork
from feed_fanout.application.ports import UnknownPost
from feed_fanout.application.use_cases import FanOutPost
from feed_fanout.config import settings
from feed_fanout.health import Health, Heartbeat, start_health_server
from feed_fanout.logs import configure_logging, get_logger
from feed_fanout.retry import DEFAULT_RETRY, RetryPolicy, is_transient
from feed_fanout.shutdown import install_signal_handlers

log = get_logger("feed_fanout.adapters.kafka.worker")


class Outcome(enum.Enum):
    FANNED_OUT = "fanned_out"
    DEAD_LETTERED = "dead_lettered"
    ABANDONED = "abandoned"  # shutdown during retries; offset not committed, will be re-delivered


class PermanentError(Exception):
    """Processing this event can never succeed; retrying is pointless."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason


def handle(value: bytes | None, fan_out_post: FanOutPost) -> int:
    """Process one event's payload. Returns feed rows added; raises PermanentError if it never can."""
    try:
        event = PostCreated.model_validate_json(value or b"")
    except ValidationError as e:
        raise PermanentError("malformed", str(e)) from e
    try:
        with metrics.FANOUT_DURATION.time():
            added = fan_out_post(event.post_id, event.author_id, event.created_at)
    except UnknownPost as e:
        raise PermanentError("unknown_post", str(e)) from e
    metrics.FEED_ROWS.observe(added)
    # What users experience: from the post's creation until it's in the feeds. (An event
    # replayed from the dead-letter topic counts from its original creation time, too.)
    metrics.POST_TO_FEED.observe(max(time.time() - event.created_at.timestamp(), 0))
    log.info("fanned out", feeds_added=added)
    return added


# Commit errors meaning "this partition was reassigned to another consumer while we were
# processing". The new owner re-processes the event from the last committed offset, which
# is safe because fanout is idempotent. Any other commit error is a real problem.
REASSIGNED_ERRORS = {
    KafkaError.ILLEGAL_GENERATION,
    KafkaError.UNKNOWN_MEMBER_ID,
    KafkaError.REBALANCE_IN_PROGRESS,
    KafkaError.STALE_MEMBER_EPOCH,
    KafkaError.FENCED_MEMBER_EPOCH,
    KafkaError._ASSIGNMENT_LOST,
}


def _event_ids(value: bytes | None) -> dict[str, str]:
    """post_id and author_id for log context; empty if the event is malformed."""
    try:
        event = PostCreated.model_validate_json(value or b"")
    except ValidationError:
        return {}
    return {"post_id": str(event.post_id), "author_id": event.author_id}


class FanoutWorker:
    def __init__(
        self,
        consumer: Consumer,
        fan_out_post: FanOutPost,
        dead_letters: DeadLetterPublisher,
        retry: RetryPolicy = DEFAULT_RETRY,
    ) -> None:
        self.consumer = consumer
        self.fan_out_post = fan_out_post
        self.dead_letters = dead_letters
        self.retry = retry
        self.heartbeat = Heartbeat()
        self._stopping = threading.Event()
        self._subscribed = False
        self._assigned_partitions = 0
        self._lag_updated_at = 0.0

    def stop(self) -> None:
        """Ask the worker to exit after the current event. Safe to call from a signal handler."""
        self._stopping.set()
        self.heartbeat.beat("stopping")

    def health(self) -> Health:
        """Live while the loop makes progress (backing off counts); ready while consuming."""
        return Health(
            live=self.heartbeat.live(),
            ready=self._subscribed and not self._stopping.is_set(),
            details={**self.heartbeat.details(), "assigned_partitions": self._assigned_partitions},
        )

    def process(self, msg: Message) -> Outcome:
        """Fan out or dead-letter one message, then commit its offset."""
        # Every log line while handling this message (retries, dead-lettering, ...) carries its
        # partition, offset and, if readable, post ids. All are unbound again afterwards.
        with bound_contextvars(partition=msg.partition(), offset=msg.offset(), **_event_ids(msg.value())):
            outcome = self._process(msg)
        metrics.EVENTS.labels(outcome=outcome.value).inc()
        return outcome

    def _process(self, msg: Message) -> Outcome:
        attempt = 0
        while True:
            attempt += 1
            try:
                handle(msg.value(), self.fan_out_post)
                outcome = Outcome.FANNED_OUT
                self.heartbeat.success()
                break
            except PermanentError as e:
                self.dead_letters.send(msg, e.reason, str(e))
                outcome = Outcome.DEAD_LETTERED
                break
            except Exception as e:
                if not is_transient(e) and attempt >= self.retry.max_unexpected_attempts:
                    log.exception("giving up", attempts=attempt)
                    self.dead_letters.send(msg, "unexpected", repr(e))
                    outcome = Outcome.DEAD_LETTERED
                    break
                self.heartbeat.failure()
                kind = "transient" if is_transient(e) else "unexpected"
                metrics.RETRIES.labels(kind=kind).inc()
                delay = self.retry.delay(attempt)
                log.warning(
                    "attempt failed, retrying",
                    attempt=attempt,
                    kind=kind,
                    error=repr(e),
                    retry_in_s=round(delay, 2),
                )
                self._wait(delay)
                if self._stopping.is_set():
                    # Shutting down mid-retry: leave the offset uncommitted so the event is
                    # re-delivered (to us after a restart, or to another worker).
                    log.info("abandoning retries for shutdown")
                    return Outcome.ABANDONED

        # Only now is the event fanned out or safely dead-lettered.
        self._commit(msg)
        return outcome

    def update_lag(self, every: float = 5.0) -> None:
        """Consumer lag per owned partition: the newest offset minus our position.

        Uses the watermarks librdkafka caches from its fetches (no extra broker calls), and
        runs at most once every `every` seconds.
        """
        now = time.monotonic()
        if now - self._lag_updated_at < every:
            return
        self._lag_updated_at = now
        metrics.CONSUMER_LAG.clear()  # drop partitions we no longer own
        for tp in self.consumer.assignment():
            _, high = self.consumer.get_watermark_offsets(tp, cached=True)
            [position] = self.consumer.position([tp])
            if high >= 0 and position.offset >= 0:
                metrics.CONSUMER_LAG.labels(partition=str(tp.partition)).set(max(high - position.offset, 0))

    def _commit(self, msg: Message) -> None:
        try:
            self.consumer.commit(message=msg, asynchronous=False)
        except KafkaException as e:
            if e.args[0].code() not in REASSIGNED_ERRORS:
                raise
            # The new owner re-processes the event; harmless, since fanout is idempotent.
            log.warning("commit rejected: partition was reassigned", error=e.args[0].name())

    def _wait(self, seconds: float) -> None:
        """Back off without being kicked out of the consumer group, and stop early on shutdown.

        Sleeping for long would exceed max.poll.interval.ms and trigger a rebalance, so pause
        every partition and keep polling instead. poll() keeps the membership alive.
        """
        self.consumer.pause(self.consumer.assignment())
        try:
            deadline = time.monotonic() + seconds
            while (remaining := deadline - time.monotonic()) > 0 and not self._stopping.is_set():
                # Waiting out an outage is correct behavior, not being stuck: stay live, so the
                # orchestrator doesn't restart us (it couldn't fix the outage anyway).
                self.heartbeat.beat("backing_off")
                msg = self.consumer.poll(min(remaining, 0.5))
                if msg is not None and not msg.error():
                    # Fetched before the pause, or from a partition assigned during it. Rewind so
                    # it's re-read later instead of silently skipped.
                    topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
                    if topic is not None and partition is not None and offset is not None:
                        self.consumer.seek(TopicPartition(topic, partition, offset))
        finally:
            # Resume what we own *now*: a rebalance during the wait may have changed it.
            self.consumer.resume(self.consumer.assignment())

    def run(self, topic: str) -> None:
        self.consumer.subscribe([topic])
        self._subscribed = True
        log.info("consuming", topic=topic)
        try:
            while not self._stopping.is_set():
                self.heartbeat.beat("consuming")
                self._assigned_partitions = len(self.consumer.assignment())
                self.update_lag()
                msg = self.consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    log.error("consumer error", error=str(msg.error()))
                    continue
                self.process(msg)
        finally:
            # Leave the group now, so partitions are reassigned immediately instead of
            # after the session timeout.
            self._subscribed = False
            self.consumer.close()
            self.dead_letters.close()
            log.info("closed")


def make_consumer(bootstrap_servers: str, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            # At-least-once: offsets are committed explicitly in process(), after handling.
            "enable.auto.commit": False,
            # Kafka's new consumer rebalance protocol (KIP-848, GA in Kafka 4.0): rebalances are
            # incremental, so only partitions that actually move are paused, not the whole group.
            "group.protocol": "consumer",
        }
    )


def main() -> None:
    configure_logging(settings.log_level, settings.log_format)
    log.info("starting", group=settings.fanout_group_id, dead_letter_topic=settings.post_created_dlt_topic)
    worker = FanoutWorker(
        make_consumer(settings.kafka_bootstrap_servers, settings.fanout_group_id),
        FanOutPost(lambda: PostgresUnitOfWork(SessionLocal, settings.post_created_topic)),
        DeadLetterPublisher(settings.kafka_bootstrap_servers, settings.post_created_dlt_topic),
    )
    install_signal_handlers(worker.stop)
    health = start_health_server(worker, settings.worker_health_port)
    try:
        worker.run(settings.post_created_topic)
    finally:
        if health:
            health.stop()


if __name__ == "__main__":
    main()
