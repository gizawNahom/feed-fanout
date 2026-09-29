"""
Outbox relay: publishes events from the `outbox` table to Kafka.

Run: uv run python -m feed_fanout.adapters.kafka.relay

The API writes a post and its PostCreated event in one transaction, so there's never a
post without an event (or the reverse). This process moves the events to Kafka:

  1. lock a batch of rows (FOR UPDATE SKIP LOCKED: other relays skip rows we hold)
  2. publish them and wait for the broker to acknowledge each one
  3. delete the acknowledged rows and commit

At-least-once: if the relay dies after step 2 but before step 3, the rows are published
again on restart. The worker's fanout is idempotent, so duplicates are harmless.
"""

import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from confluent_kafka import KafkaError, Message, Producer
from prometheus_client import REGISTRY
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector
from sqlalchemy import delete, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout import metrics
from feed_fanout.adapters.kafka.producers import close_producer, make_producer
from feed_fanout.adapters.postgres.db import SessionLocal
from feed_fanout.adapters.postgres.models import OutboxEvent
from feed_fanout.config import settings
from feed_fanout.health import Health, Heartbeat, start_health_server
from feed_fanout.logs import configure_logging, get_logger
from feed_fanout.retry import DEFAULT_RETRY, RetryPolicy
from feed_fanout.shutdown import install_signal_handlers

log = get_logger("feed_fanout.adapters.kafka.relay")


@dataclass(frozen=True)
class BatchResult:
    published: int
    failed: int


class OutboxRelay:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        producer: Producer,
        batch_size: int = 100,
        flush_timeout: float = 15.0,  # must exceed the producer's message.timeout.ms (10s)
    ) -> None:
        self.session_factory = session_factory
        self.producer = producer
        self.batch_size = batch_size
        self.flush_timeout = flush_timeout
        self.heartbeat = Heartbeat()
        self._stopping = threading.Event()
        self._running = False
        self._closed = False

    def stop(self) -> None:
        """Ask the relay to exit after the current batch. Safe to call from a signal handler."""
        self._stopping.set()
        self.heartbeat.beat("stopping")

    def health(self) -> Health:
        """Live while the loop makes progress (backing off counts); ready while running."""
        return Health(
            live=self.heartbeat.live(),
            ready=self._running and not self._stopping.is_set(),
            details=self.heartbeat.details(),
        )

    def publish_batch(self) -> BatchResult:
        """Publish up to batch_size pending events. Only acknowledged events are removed."""
        with self.session_factory.begin() as session:
            rows = session.scalars(
                select(OutboxEvent).order_by(OutboxEvent.id).limit(self.batch_size).with_for_update(skip_locked=True)
            ).all()
            if not rows:
                return BatchResult(0, 0)

            errors: dict[int, KafkaError | None] = {}
            for row in rows:
                self.producer.produce(
                    row.topic,
                    key=row.key,
                    value=row.payload,
                    on_delivery=self._record(errors, row.id),
                )
            # message.timeout.ms < flush_timeout, so every message is resolved (delivered or
            # failed) by now, and none linger to be sent again with the next batch.
            self.producer.flush(self.flush_timeout)

            delivered = [row.id for row in rows if row.id in errors and errors[row.id] is None]
            if delivered:
                session.execute(delete(OutboxEvent).where(OutboxEvent.id.in_(delivered)))
            failed = len(rows) - len(delivered)
            for row in rows:
                if errors.get(row.id) is not None:
                    log.warning(
                        "publish failed", outbox_id=row.id, topic=row.topic, key=row.key, error=str(errors[row.id])
                    )
        # Committed: acknowledged rows are gone; failed ones stay for the next attempt.
        metrics.RELAY_PUBLISHED.inc(len(delivered))
        metrics.RELAY_FAILED.inc(failed)
        if delivered:
            log.info("published", events=len(delivered))
        return BatchResult(len(delivered), failed)

    @staticmethod
    def _record(errors: dict[int, KafkaError | None], row_id: int) -> Callable[[KafkaError | None, Message], None]:
        def on_delivery(err: KafkaError | None, _msg: Message) -> None:
            errors[row_id] = err

        return on_delivery

    def run(self, idle_interval: float = 0.5, retry: RetryPolicy = DEFAULT_RETRY) -> None:
        """Publish until stopped. Polls every idle_interval when there's nothing to do."""
        log.info("relaying outbox")
        self._running = True
        try:
            while not self._stopping.is_set():
                self.heartbeat.beat("publishing")
                try:
                    result = self.publish_batch()
                    ok = result.failed == 0
                except Exception:
                    log.exception("relay batch failed")
                    result, ok = BatchResult(0, 0), False
                if not ok:
                    self.heartbeat.failure()
                    failures = self.heartbeat.consecutive_failures
                    delay = retry.delay(failures)
                    log.warning("backing off", retry_in_s=round(delay, 2), consecutive_failures=failures)
                    # Backing off is progress, not being stuck (delays are capped well below
                    # the heartbeat's stale_after).
                    self.heartbeat.beat("backing_off")
                    self._stopping.wait(delay)  # like sleep, but returns early on stop()
                    continue
                self.heartbeat.success()
                if result.published < self.batch_size:  # caught up: wait for new events
                    self.heartbeat.beat("idle")
                    self._stopping.wait(idle_interval)
        finally:
            self._running = False
            self.close()

    def close(self) -> None:
        """Deliver anything buffered and release the producer. Safe to call twice."""
        if self._closed:
            return
        self._closed = True
        self.producer.flush(self.flush_timeout)
        close_producer(self.producer)
        log.info("closed")


class OutboxCollector(Collector):
    """Outbox backlog and the age of its oldest event, read from the database on each scrape.

    The age matters more than the count: a thousand events published within a second is
    fine; one event stuck for five minutes means the relay (or Kafka) is stuck.
    """

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self.session_factory = session_factory

    def describe(self) -> Iterable[Metric]:
        return []  # don't query the database when registering

    def collect(self) -> Iterable[Metric]:
        try:
            with self.session_factory() as session:
                count, oldest = session.execute(select(func.count(), func.min(OutboxEvent.created_at))).one()
        except SQLAlchemyError as e:
            # Leave the gauges out rather than failing the whole scrape.
            log.warning("outbox metrics unavailable", error=e.__class__.__name__)
            return
        age = 0.0 if oldest is None else max((datetime.now(UTC) - oldest).total_seconds(), 0.0)
        yield GaugeMetricFamily("feed_fanout_outbox_backlog", "Outbox events waiting to be published.", value=count)
        yield GaugeMetricFamily(
            "feed_fanout_outbox_oldest_age_seconds", "Age of the oldest outbox event (0 when empty).", value=age
        )


def main() -> None:
    configure_logging(settings.log_level, settings.log_format)
    relay = OutboxRelay(SessionLocal, make_producer(settings.kafka_bootstrap_servers))
    install_signal_handlers(relay.stop)
    REGISTRY.register(OutboxCollector(SessionLocal))
    health = start_health_server(relay, settings.relay_health_port)
    try:
        relay.run()
    finally:
        if health:
            health.stop()


if __name__ == "__main__":
    main()
