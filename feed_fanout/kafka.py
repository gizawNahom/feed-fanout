import logging
from datetime import UTC, datetime

from confluent_kafka import KafkaError, KafkaException, Message, Producer

log = logging.getLogger(__name__)


def make_producer(bootstrap_servers: str, message_timeout_ms: int = 10_000) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        # Match Java clients' murmur2 hashing so a key always maps to the same partition.
        "partitioner": "murmur2_random",
        # No duplicates or reordering from producer retries (implies acks=all).
        "enable.idempotence": True,
        # Give up on a message after this long (librdkafka retries internally until then),
        # so a flush() that waits a bit longer always ends with every message resolved.
        "message.timeout.ms": message_timeout_ms,
    })


class DeadLetterPublisher:
    """Moves events that can't be processed to a dead-letter topic, for inspection and replay.

    The original key and value are kept unchanged; why it failed and where it came from
    go in headers.
    """

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic
        self._producer = make_producer(bootstrap_servers)

    def send(self, msg: Message, reason: str, error: str) -> None:
        """Publish synchronously: returns only once the broker has stored it, raises otherwise.

        The caller commits the original offset right after, so the event must be safely
        in the dead-letter topic first, or it would be lost.
        """
        headers = list(msg.headers() or []) + [
            ("dlt.reason", reason),
            ("dlt.error", error[:1000]),
            ("dlt.original.topic", msg.topic()),
            ("dlt.original.partition", str(msg.partition())),
            ("dlt.original.offset", str(msg.offset())),
            ("dlt.failed_at", datetime.now(UTC).isoformat()),
        ]
        errors: list[KafkaError] = []
        self._producer.produce(
            self.topic, key=msg.key(), value=msg.value(), headers=headers,
            on_delivery=lambda err, _: errors.append(err) if err else None,
        )
        if self._producer.flush(15) or errors:
            raise KafkaException(errors[0] if errors else KafkaError(KafkaError._TIMED_OUT))
        log.warning("dead-lettered reason=%s topic=%s partition=%s offset=%s error=%s",
                    reason, msg.topic(), msg.partition(), msg.offset(), error)

    def close(self) -> None:
        self._producer.flush(10)
        self._producer.close()
