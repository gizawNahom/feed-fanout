import logging

from confluent_kafka import KafkaError, Message, Producer

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
