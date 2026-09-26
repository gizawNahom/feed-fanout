"""
Publishes "post created" events to Kafka.

This is v1's create_post(), except the queue is now the Kafka topic
"post-created" instead of an in-memory queue.Queue.
"""

import json
import time
import uuid

from confluent_kafka import KafkaError, Message, Producer

TOPIC = "post-created"

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    # librdkafka hashes keys with CRC32 by default; Java clients use murmur2.
    # Match Java so the same key lands on the same partition no matter which client produced it.
    "partitioner": "murmur2_random",
})


def report(err: KafkaError | None, msg: Message) -> None:
    """Delivery callback: called once the broker has stored (or rejected) a message."""
    event = json.loads(msg.value())
    if err is not None:
        print(f"FAILED  {event['post_id']} ({event['author']}): {err}")
    else:
        print(f"delivered {event['post_id']} ({event['author']}) -> partition {msg.partition()}, offset {msg.offset()}")


def create_post(author: str, text: str) -> str:
    post_id = uuid.uuid4().hex[:6]
    event = {"post_id": post_id, "author": author, "text": text, "ts": time.time()}

    producer.produce(
        topic=TOPIC,
        key=author,                 # same author -> same partition -> ordered
        value=json.dumps(event),    # Kafka stores bytes; we use JSON
        on_delivery=report,
    )
    producer.poll(0)                # serve delivery callbacks for earlier messages, without blocking
    print(f"queued    {post_id} ({author})")
    return post_id


def main() -> None:
    create_post("alice", "hello world")
    create_post("bob", "first post!")
    create_post("alice", "lunch was great")
    create_post("carol", "anyone up for chess?")
    create_post("bob", "queues are neat")

    # Anything still in the local buffer is lost if we exit now, so wait for it.
    remaining = producer.flush(10)
    if remaining:
        print(f"{remaining} message(s) were NOT delivered")


if __name__ == "__main__":
    main()
