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
| [v3-service](https://github.com/gizawNahom/feed-fanout/tree/v3-service) | FastAPI + Postgres (posts, follows, feeds) + Alembic migrations + integration tests | A real API; feeds are shared, persistent, and paginated |
| v4-reliable *(next)* | Retry/dead-letter topics, outbox, graceful shutdown | Never lose or duplicate a post |
| v5-ops *(planned)* | CI, logging, health checks, Docker, 3-broker Kafka | Production readiness |
| v6-scale *(planned)* | Metrics, load testing, caching, celebrity handling | Measured performance |

## Quick start

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/gizawNahom/feed-fanout.git
cd feed-fanout

# 1. Start Kafka, Postgres, and the dashboards
docker compose up -d

# 2. One-time setup: create the topics and the database tables
docker exec feed-fanout-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic post-created --partitions 3 --replication-factor 1
docker exec feed-fanout-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic post-created.dlt --partitions 1 --replication-factor 1
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

## Failure handling

The worker commits an event's offset only once it has been fanned out **or** safely moved to the dead-letter topic. So every event ends up in exactly one of those two places.

| Failure | Example | What the worker does |
|---|---|---|
| **Permanent** | Malformed event, post doesn't exist | Publishes the event to **`post-created.dlt`** unchanged, with `dlt.*` headers (reason, error, original topic/partition/offset, time), then moves on |
| **Transient** | Database down, connection lost, deadlock | **Pauses consumption and retries the same event** with exponential backoff (1s → 30s), indefinitely |
| **Unexpected** | A bug | Retries 3 times, then dead-letters it with reason `unexpected` |

**Why no retry topic?** Retry topics suit failures that hit *individual* events (say, one rate-limited API call). Our transient failure is the database being down, which is systemic: while it lasts, no event can succeed. Moving events to a retry topic would drain the whole backlog into it and break per-author ordering, for nothing. Pausing and retrying in place loses nothing and keeps order. While backing off, the worker keeps polling with its partitions paused, so Kafka doesn't evict it from the group for being too slow (`max.poll.interval.ms`).

Inspect dead letters in Kafka UI (topic `post-created.dlt`, headers included), or with:

```bash
docker exec feed-fanout-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic post-created.dlt --from-beginning --formatter-property print.headers=true --formatter-property print.key=true
```

## Tests

```bash
uv run pytest
```

Integration tests run against **real Postgres and Kafka** in throwaway containers ([testcontainers](https://testcontainers.com/)), using the same images as `docker-compose.yml`. Your dev containers and data are never touched. Each test gets empty tables and its own Kafka topic. The schema is built by running the real Alembic migrations, so the migrations are tested too. Docker must be running.

They cover the behavior that matters: followers receive posts and non-followers don't; replaying an event adds no duplicates; bad events land in the dead-letter topic with their context, and the worker moves on; a database outage is retried until it succeeds; feeds are newest-first; and pagination returns every post exactly once, even when timestamps tie.

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
- **Database outage:** stop the worker, create a post, run `docker compose stop postgres`, and start the worker. It backs off (1s, 2s, 4s…). Run `docker compose start postgres` and the post is fanned out.
- **Dead letters:** produce garbage to `post-created` with the console producer and watch it land in `post-created.dlt`.

## What's worth knowing

- **Thin events.** The Kafka event carries only `post_id`, `author_id`, and `created_at`. The post itself lives in Postgres, the single source of truth.
- **Fanout is one SQL statement.** `INSERT INTO feed_items ... SELECT FROM follows ... ON CONFLICT DO NOTHING`. Postgres reads the followers and writes the feed rows itself, without loading anything into Python.
- **At-least-once + idempotency.** The worker commits a Kafka offset only after the fanout transaction commits. If it crashes in between, the event is re-delivered, and the `(user_id, post_id)` primary key makes the re-run a no-op.
- **Keyset pagination.** Feeds page by `(created_at, post_id) < cursor` instead of `OFFSET`, so pages stay stable while new posts arrive, and deep pages stay fast.
- **Keyed partitioning across clients.** librdkafka hashes keys with CRC32 by default, while Java clients use murmur2. The producer sets `partitioner: murmur2_random` so a key always maps to the same partition.
- **Idempotent producer.** `enable.idempotence` prevents the producer's own retries from duplicating or reordering messages.

## Current limitations

- **Dual write.** The API commits the post and then publishes the event. If it crashes in between, the post is never fanned out. *(v4: outbox pattern)*
- **No tooling to replay dead letters** back into the main topic yet.
- **Workers don't handle SIGTERM.** A killed worker never leaves its group, so its partitions stall until the session timeout (~45s). *(v4)*
- **Single Kafka broker; services run outside Docker.** *(v5)*

## License

[MIT](LICENSE)
