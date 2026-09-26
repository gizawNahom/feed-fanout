"""
Fanout worker: reads "post created" events from Kafka and writes each post
into every follower's feed.

This is v1's worker() loop, except messages come from the Kafka topic
"post-created" instead of an in-memory queue.Queue.

Known limitations (fixed in v3): the social graph is hardcoded, and feeds live
only in this process's memory. They're lost on restart, and with several
consumers each one holds only part of every feed.
"""

import json
import os
from collections import defaultdict

from confluent_kafka import Consumer, Message

TOPIC = "post-created"

# Hardcoded social graph: author -> users who follow them.
FOLLOWERS = {
    "alice": ["bob", "carol", "dave", "erin"],
    "bob": ["alice", "carol"],
    "carol": ["alice"],
}

# In-memory feeds: user -> {post_id: post}.
# Keyed by post_id so re-processing a message (at-least-once) doesn't duplicate it.
feeds: dict[str, dict[str, dict]] = defaultdict(dict)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    # Consumers sharing a group.id split the topic's partitions between them.
    "group.id": os.environ.get("GROUP_ID", "fanout-workers"),
    # Where a brand-new group starts: from the oldest message still in the topic.
    "auto.offset.reset": "earliest",
    # At-least-once: we commit an offset only after its fanout is done (see main()).
    # Auto-commit would commit on a timer, whether or not processing finished,
    # so a crash could skip a post (committed, never fanned out).
    "enable.auto.commit": False,
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
    post = {"post_id": post_id, "author": author, "text": text, "ts": event.get("ts", 0)}

    followers = FOLLOWERS.get(author, [])
    for follower in followers:
        feeds[follower][post_id] = post  # idempotent: same post twice = one entry
    print(f"{where}: {post_id} by {author} -> {', '.join(followers) or '(no followers)'}")


def print_feeds() -> None:
    print("\n=== Feeds held by this consumer ===")
    for user in sorted(feeds):
        print(f"\n{user}:")
        # Newest first. Posts from different partitions arrive in any order, so sort by timestamp.
        for p in sorted(feeds[user].values(), key=lambda p: p["ts"], reverse=True)[:10]:
            print(f"  [{p['post_id']}] {p['author']}: {p['text']}")


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
            # Fanout is done: now it's safe to mark the message as processed.
            # If we crash before this line, Kafka re-delivers the message (handled idempotently).
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave the group so partitions are reassigned right away.
        consumer.close()
        print("closed")
        print_feeds()


if __name__ == "__main__":
    main()
