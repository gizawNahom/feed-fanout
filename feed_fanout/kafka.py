import logging

from confluent_kafka import KafkaError, Message, Producer

from feed_fanout.config import KAFKA_BOOTSTRAP_SERVERS, POST_CREATED_TOPIC
from feed_fanout.events import PostCreated

log = logging.getLogger(__name__)

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    # Match Java clients' murmur2 hashing so a key always maps to the same partition.
    "partitioner": "murmur2_random",
    # No duplicates or reordering from producer retries (implies acks=all).
    "enable.idempotence": True,
})


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    key = msg.key().decode() if msg.key() else None
    if err is not None:
        log.error("publish failed topic=%s key=%s error=%s", msg.topic(), key, err)
    else:
        log.info("published topic=%s key=%s partition=%s offset=%s",
                 msg.topic(), key, msg.partition(), msg.offset())


def publish_post_created(event: PostCreated) -> None:
    producer.produce(
        topic=POST_CREATED_TOPIC,
        key=event.author_id,  # same author -> same partition -> ordered
        value=event.model_dump_json(),
        on_delivery=_on_delivery,
    )
    producer.poll(0)  # serve delivery callbacks without blocking
