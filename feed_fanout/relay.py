"""
Outbox relay: publishes events from the `outbox` table to Kafka.

Run: uv run python -m feed_fanout.relay

The API writes a post and its PostCreated event in one transaction, so there's never a
post without an event (or the reverse). This process moves the events to Kafka:

  1. lock a batch of rows (FOR UPDATE SKIP LOCKED: other relays skip rows we hold)
  2. publish them and wait for the broker to acknowledge each one
  3. delete the acknowledged rows and commit

At-least-once: if the relay dies after step 2 but before step 3, the rows are published
again on restart. The worker's fanout is idempotent, so duplicates are harmless.
"""

import logging
import time
from dataclasses import dataclass

from confluent_kafka import KafkaError, Message, Producer
from sqlalchemy import delete, select
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.config import KAFKA_BOOTSTRAP_SERVERS
from feed_fanout.db import SessionLocal
from feed_fanout.kafka import make_producer
from feed_fanout.models import OutboxEvent
from feed_fanout.retry import RetryPolicy

log = logging.getLogger("feed_fanout.relay")


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

    def publish_batch(self) -> BatchResult:
        """Publish up to batch_size pending events. Only acknowledged events are removed."""
        with self.session_factory.begin() as session:
            rows = session.scalars(
                select(OutboxEvent)
                .order_by(OutboxEvent.id)
                .limit(self.batch_size)
                .with_for_update(skip_locked=True)
            ).all()
            if not rows:
                return BatchResult(0, 0)

            errors: dict[int, KafkaError | None] = {}
            for row in rows:
                self.producer.produce(
                    row.topic, key=row.key, value=row.payload,
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
                    log.warning("publish failed outbox_id=%s topic=%s key=%s error=%s",
                                row.id, row.topic, row.key, errors[row.id])
        # Committed: acknowledged rows are gone; failed ones stay for the next attempt.
        if delivered:
            log.info("published %d event(s)", len(delivered))
        return BatchResult(len(delivered), failed)

    @staticmethod
    def _record(errors: dict[int, KafkaError | None], row_id: int):
        def on_delivery(err: KafkaError | None, _msg: Message) -> None:
            errors[row_id] = err
        return on_delivery

    def run(self, idle_interval: float = 0.5, retry: RetryPolicy = RetryPolicy()) -> None:
        """Publish until interrupted. Polls every idle_interval when there's nothing to do."""
        log.info("relaying outbox")
        failures = 0
        try:
            while True:
                try:
                    result = self.publish_batch()
                    ok = result.failed == 0
                except Exception:
                    log.exception("relay batch failed")
                    result, ok = BatchResult(0, 0), False
                if not ok:
                    failures += 1
                    delay = retry.delay(failures)
                    log.warning("backing off %.1fs (consecutive failures: %d)", delay, failures)
                    time.sleep(delay)
                    continue
                failures = 0
                if result.published < self.batch_size:  # caught up: wait for new events
                    time.sleep(idle_interval)
        except KeyboardInterrupt:
            pass
        finally:
            self.producer.flush(self.flush_timeout)
            self.producer.close()
            log.info("closed")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    OutboxRelay(SessionLocal, make_producer(KAFKA_BOOTSTRAP_SERVERS)).run()


if __name__ == "__main__":
    main()
