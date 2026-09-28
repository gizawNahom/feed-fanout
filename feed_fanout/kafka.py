import logging
from datetime import UTC, datetime

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from feed_fanout.config import POST_CREATED_TOPIC
from feed_fanout.events import PostCreated

log = logging.getLogger(__name__)


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    key = msg.key().decode() if msg.key() else None
    if err is not None:
        log.error("publish failed topic=%s key=%s error=%s", msg.topic(), key, err)
    else:
        log.info("published topic=%s key=%s partition=%s offset=%s",
                 msg.topic(), key, msg.partition(), msg.offset())


class PostCreatedPublisher:
    def __init__(self, bootstrap_servers: str, topic: str = POST_CREATED_TOPIC) -> None:
        self.topic = topic
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            # Match Java clients' murmur2 hashing so a key always maps to the same partition.
            "partitioner": "murmur2_random",
            # No duplicates or reordering from producer retries (implies acks=all).
            "enable.idempotence": True,
        })

    def publish(self, event: PostCreated) -> None:
        self._producer.produce(
            topic=self.topic,
            key=event.author_id,  # same author -> same partition -> ordered
            value=event.model_dump_json(),
            on_delivery=_on_delivery,
        )
        self._producer.poll(0)  # serve delivery callbacks without blocking

    def flush(self, timeout: float) -> int:
        """Wait for buffered messages to be delivered. Returns how many are still undelivered."""
        return self._producer.flush(timeout)

    def close(self) -> None:
        """Deliver what's buffered, then release the producer's connections and threads."""
        self.flush(10)
        self._producer.close()


class DeadLetterPublisher:
    """Moves events that can't be processed to a dead-letter topic, for inspection and replay.

    The original key and value are kept unchanged; why it failed and where it came from
    go in headers.
    """

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic
        self._producer = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True})

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
        if self._producer.flush(10) or errors:
            raise KafkaException(errors[0] if errors else KafkaError(KafkaError._TIMED_OUT))
        log.warning("dead-lettered reason=%s topic=%s partition=%s offset=%s error=%s",
                    reason, msg.topic(), msg.partition(), msg.offset(), error)

    def close(self) -> None:
        self._producer.flush(10)
        self._producer.close()
