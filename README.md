# feed-fanout

How Facebook's News Feed delivers posts, built step by step: from an in-memory queue to a production-grade, fault-tolerant pipeline.

System design interviews talk about queues, workers, and "fanout-on-write" through boxes and arrows. This repo turns that diagram into code you can run. The project grows in tagged versions. Each version adds one layer and fixes what broke in the one before it.

## The design

When a user posts, we don't build everyone's feed on the spot. We save the post, publish a "post created" event, and return immediately. Fanout workers consume the events and copy each post into every follower's precomputed feed, so reading a feed is a cheap indexed lookup.

```
POST /posts ──► API ──► Postgres: posts
                 │
                 └──► Kafka "post-created" ──► fanout worker (×N) ──► Postgres: feed_items
                      (key = author)            look up followers,       one row per
                                                insert feed rows         (follower, post)

GET /users/{id}/feed ──► API ──► Postgres: feed_items ⨝ posts (newest first)
```

**The trade-off:** posting is fast and reads are cheap, but feeds are *eventually consistent*. A new post takes a moment to reach every follower.

## Evolution

`main` always holds the latest version. Each version is a git tag you can browse or check out.

| Version | What it adds | What it fixes |
|---|---|---|
| [v1-in-memory](https://github.com/gizawNahom/feed-fanout/tree/v1-in-memory) | `queue.Queue` + worker threads in one process | Proves the pattern: decouple posting from slow fanout |
| [v2-kafka](https://github.com/gizawNahom/feed-fanout/tree/v2-kafka) | Kafka as the queue; separate producer and worker processes; at-least-once commits | Queued work survives crashes; workers scale independently |
| **v3-service** *(in progress)* | FastAPI + Postgres (posts, follows, feeds) + Alembic migrations | A real API; feeds are shared, persistent, and paginated |
| v4-reliable *(planned)* | Retry/dead-letter topics, outbox, graceful shutdown | Never lose or duplicate a post |
| v5-ops *(planned)* | CI, logging, health checks, Docker, 3-broker Kafka | Production readiness |
| v6-scale *(planned)* | Metrics, load testing, caching, celebrity handling | Measured performance |

## Quick start

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/gizawNahom/feed-fanout.git
cd feed-fanout

# 1. Start Kafka, Postgres, and the dashboards
docker compose up -d

# 2. One-time setup: create the topic and the database tables
docker exec feed-fanout-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic post-created --partitions 3 --replication-factor 1
uv run alembic upgrade head

# 3. Run the API (terminal 1) and a fanout worker (terminal 2)
uv run uvicorn feed_fanout.api:app --reload
uv run python -m feed_fanout.worker
```

Try it:

```bash
curl -X PUT localhost:8000/users/bob/following/alice                  # bob follows alice
curl -X POST localhost:8000/posts -H 'content-type: application/json' \
  -d '{"author_id": "alice", "text": "hello world"}'                   # alice posts
curl localhost:8000/users/bob/feed                                    # bob's feed
```

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/posts` | Create a post `{author_id, text}` → `201` with the post |
| `PUT` | `/users/{id}/following/{followee}` | Follow (idempotent) → `204` |
| `DELETE` | `/users/{id}/following/{followee}` | Unfollow → `204` |
| `GET` | `/users/{id}/feed?limit=20&cursor=…` | Feed, newest first. Pass `next_cursor` from the response to get older posts. |

## Observability

| What | Where |
|---|---|
| API docs, with try-it-out | http://localhost:8000/docs |
| Kafka UI: messages, partitions, consumer lag | http://localhost:8080 |
| Adminer: browse `posts`, `follows`, `feed_items` | http://localhost:8081 (System: PostgreSQL, Server: `postgres`, user/password/database: `feed`) |
| Logs | Every log line carries the `post_id`, so one post can be followed from the API through Kafka to the worker |

## Things to try

- **Scale out:** start a second worker. Kafka splits the partitions between them.
- **Durability:** stop the worker, create posts, and start it again. The posts waited in Kafka, and the feeds catch up.
- **Idempotency:** rewind the consumer group and replay events. The worker logs `feeds_added=0`, and no feed gets a duplicate.

## What's worth knowing

- **Thin events.** The Kafka event carries only `post_id`, `author_id`, and `created_at`. The post itself lives in Postgres, the single source of truth.
- **Fanout is one SQL statement.** `INSERT INTO feed_items ... SELECT FROM follows ... ON CONFLICT DO NOTHING`. Postgres reads the followers and writes the feed rows itself, without loading anything into Python.
- **At-least-once + idempotency.** The worker commits a Kafka offset only after the fanout transaction commits. If it crashes in between, the event is re-delivered, and the `(user_id, post_id)` primary key makes the re-run a no-op.
- **Keyset pagination.** Feeds page by `(created_at, post_id) < cursor` instead of `OFFSET`, so pages stay stable while new posts arrive, and deep pages stay fast.
- **Keyed partitioning across clients.** librdkafka hashes keys with CRC32 by default, while Java clients use murmur2. The producer sets `partitioner: murmur2_random` so a key always maps to the same partition.
- **Idempotent producer.** `enable.idempotence` prevents the producer's own retries from duplicating or reordering messages.

## Current limitations

- **Dual write.** The API commits the post and then publishes the event. If it crashes in between, the post is never fanned out. *(v4: outbox pattern)*
- **Malformed events are dropped**, not kept for inspection. *(v4: dead-letter topic)*
- **Workers don't handle SIGTERM.** A killed worker never leaves its group, so its partitions stall until the session timeout (~45s). *(v4)*
- **No automated tests yet.** *(v3, next step)*
- **Single Kafka broker; services run outside Docker.** *(v5)*

## License

[MIT](LICENSE)
