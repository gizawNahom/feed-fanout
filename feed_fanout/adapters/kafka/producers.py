from datetime import UTC, datetime

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from feed_fanout import metrics
from feed_fanout.logs import get_logger

log = get_logger(__name__)

REPLAY_COUNT_HEADER = "dlt.replay_count"  # how many times an event was replayed from the DLT

Headers = list[tuple[str, str | bytes | None]]


def headers_of(msg: Message) -> list[tuple[str, bytes]]:
    """A consumed message's headers. (The type stubs also allow a dict; consumed messages give a list.)"""
    raw = msg.headers() or []
    items = raw.items() if isinstance(raw, dict) else raw
    return [(k, v if isinstance(v, bytes) else (v or "").encode()) for k, v in items]


def close_producer(producer: Producer) -> None:
    """Release the producer's connections and threads.

    Producer.close() exists at runtime but is missing from confluent-kafka's type stubs.
    """
    producer.close()  # type: ignore[attr-defined]


def make_producer(bootstrap_servers: str, message_timeout_ms: int = 10_000) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            # Match Java clients' murmur2 hashing so a key always maps to the same partition.
            "partitioner": "murmur2_random",
            # No duplicates or reordering from producer retries (implies acks=all).
            "enable.idempotence": True,
            # Give up on a message after this long (librdkafka retries internally until then),
            # so a flush() that waits a bit longer always ends with every message resolved.
            "message.timeout.ms": message_timeout_ms,
        }
    )


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
        # Keep the event's own headers, but not dlt.* ones from an earlier dead-lettering
        # (a replayed event that failed again), except the replay count, which travels with it.
        kept = [(k, v) for k, v in headers_of(msg) if not k.startswith("dlt.") or k == REPLAY_COUNT_HEADER]
        headers: Headers = [
            *kept,
            ("dlt.reason", reason),
            ("dlt.error", error[:1000]),
            ("dlt.original.topic", msg.topic()),
            ("dlt.original.partition", str(msg.partition())),
            ("dlt.original.offset", str(msg.offset())),
            ("dlt.failed_at", datetime.now(UTC).isoformat()),
        ]
        errors: list[KafkaError] = []
        self._producer.produce(
            self.topic,
            key=msg.key(),
            value=msg.value(),
            headers=headers,
            on_delivery=lambda err, _: errors.append(err) if err else None,
        )
        if self._producer.flush(15) or errors:
            raise KafkaException(errors[0] if errors else KafkaError(KafkaError._TIMED_OUT))
        metrics.DEAD_LETTERS.labels(reason=reason).inc()
        log.warning("dead-lettered", reason=reason, dead_letter_topic=self.topic, error=error)

    def close(self) -> None:
        self._producer.flush(10)
        close_producer(self._producer)
