# feed-fanout

How Facebook's News Feed delivers posts, built step by step: from an in-memory queue to a production-grade, fault-tolerant pipeline.

System design interviews talk about queues, workers, and "fanout-on-write" through boxes and arrows. This repo turns that diagram into code you can run. The project grows in tagged versions. Each version adds one layer and fixes what broke in the one before it.

## The design

When a user posts, we don't build everyone's feed on the spot. We publish a "post created" event and return immediately. Fanout workers consume the events and copy each post into every follower's precomputed feed, so reading a feed is a cheap lookup.

```
producer.py ──► Kafka topic "post-created" ──► consumer.py (×N, one consumer group)
 create_post()    3 partitions, key = author      look up followers → write to their feeds
```

**The trade-off:** posting is fast and reads are cheap, but feeds are *eventually consistent*. A new post takes a moment to reach every follower.

## Evolution

`main` always holds the latest version. Each version is a git tag you can browse or check out.

| Version | What it adds | What it fixes |
|---|---|---|
| [v1-in-memory](https://github.com/gizawNahom/feed-fanout/tree/v1-in-memory) | `queue.Queue` + worker threads in one process | Proves the pattern: decouple posting from slow fanout |
| **v2-kafka** | Kafka as the queue; producer and fanout worker as separate processes | Queued work survives crashes; workers scale independently |
| v3-service *(next)* | FastAPI + Postgres | Feeds become shared and persistent; a real API |
| v4-reliable *(planned)* | Manual commits, idempotency, retry/dead-letter topics, outbox | Never lose or duplicate a post |
| v5-ops *(planned)* | CI, logging, health checks, Docker, 3-broker Kafka | Production readiness |
| v6-scale *(planned)* | Metrics, load testing, caching, celebrity handling | Measured performance |

## Quick start

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/gizawNahom/feed-fanout.git
cd feed-fanout

# 1. Start Kafka and Kafka UI
docker compose up -d

# 2. Create the topic (once)
docker exec feed-fanout-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic post-created --partitions 3 --replication-factor 1

# 3. Start a fanout worker (terminal 1)
uv run python consumer.py

# 4. Publish some posts (terminal 2)
uv run python producer.py
```

Press Ctrl+C in the consumer to stop it. It prints the feeds it built.

**Kafka UI** runs at http://localhost:8080. Use it to browse messages, see which partition each author lands on, and watch the `fanout-workers` consumer group's lag.

## Things to try

- **Scale out:** start a second `consumer.py`. Kafka splits the 3 partitions between the two workers, and each one processes only its own share.
- **Durability:** stop the consumer, run the producer, then start the consumer again. The posts waited in Kafka.
- **Ordering:** posts are keyed by author, so each author's posts always go to the same partition and stay in order.

## What's worth knowing

- **Keyed partitioning across clients.** librdkafka (used by `confluent-kafka`) hashes keys with CRC32 by default, while Java clients use murmur2. The producer sets `partitioner: murmur2_random` so a key maps to the same partition no matter which client produced it.
- **Delivery callbacks.** `produce()` is asynchronous. It only buffers the message locally. The `on_delivery` callback is the only way to learn whether the broker stored it, and `flush()` before exit prevents buffered messages from being lost.
- **Malformed messages.** A message the consumer can't parse is logged and skipped instead of crashing the worker. Otherwise it would crash on the same message forever.

## Current limitations

- **Feeds live in the consumer's memory.** They're lost on restart, and with several consumers each one holds only part of every feed. Nothing else can read them. *(v3: Postgres)*
- **The social graph is hardcoded.** *(v3)*
- **Offsets are auto-committed** on a timer, whether or not fanout finished. A crash can skip or repeat work. *(v4)*
- **Malformed messages are dropped**, not kept for inspection. *(v4: dead-letter topic)*
- **Single Kafka broker.** *(v5)*

## License

[MIT](LICENSE)
