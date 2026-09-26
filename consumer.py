"""
Reads "post created" events from Kafka.

This is v1's worker() loop, except messages come from the Kafka topic
"post-created" instead of an in-memory queue.Queue. For now it only prints
what it reads; fanout comes next.
"""

import json
import os

from confluent_kafka import Consumer, Message

TOPIC = "post-created"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    # Consumers sharing a group.id split the topic's partitions between them.
    "group.id": os.environ.get("GROUP_ID", "fanout-workers"),
    # Where a brand-new group starts: from the oldest message still in the topic.
    "auto.offset.reset": "earliest",
    # Offsets are auto-committed every 5s by default, even if processing didn't finish.
    # Known weakness; fixed in a later step with manual commits.
})


def handle(msg: Message) -> None:
    where = f"partition {msg.partition()} offset {msg.offset()}"
    try:
        event = json.loads(msg.value())
        post_id, author = event["post_id"], event["author"]
    except (json.JSONDecodeError, TypeError, KeyError) as e:
        # A malformed message must not crash the consumer, or it would crash on the
        # same message forever. For now: log and skip. Later: send to a dead-letter topic.
        print(f"{where}: SKIPPED malformed message {msg.value()!r} ({e!r})")
        return
    # "text" was added after the first messages were produced, so older events don't have it.
    text = event.get("text", "")
    print(f"{where}: {post_id} by {author}: {text!r}")


def main() -> None:
    consumer.subscribe([TOPIC])
    print(f"consuming {TOPIC!r}... (Ctrl+C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0)  # wait up to 1s for the next message
            if msg is None:
                continue
            if msg.error():
                print(f"error: {msg.error()}")
                continue
            handle(msg)
    except KeyboardInterrupt:
        pass
    finally:
        # Commit final offsets and leave the group so partitions are reassigned right away.
        consumer.close()
        print("closed")


if __name__ == "__main__":
    main()
