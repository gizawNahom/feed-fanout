# feed-fanout

How Facebook's News Feed delivers posts, built step by step: from an in-memory queue to a production-grade, fault-tolerant pipeline.

System design interviews talk about queues, workers, and "fanout-on-write" through boxes and arrows. This repo turns that diagram into code you can run. The project grows in tagged versions. Each version adds one layer and fixes what broke in the one before it.

## The design

When a user posts, we don't build everyone's feed on the spot. We save the post together with a "post created" event and return immediately. A relay publishes the event to Kafka; fanout workers consume it and copy the post into every follower's precomputed feed, so reading a feed is a cheap indexed lookup.

```
                     one transaction
POST /posts ──► API ──► Postgres: posts + outbox
                                         │
                                  outbox relay (polls, publishes, deletes)
                                         │
                                         ▼
                        Kafka "post-created" ──► fanout worker (×N) ──► Postgres: feed_items
                        (key = author)            look up followers,      one row per
                                                  insert feed rows        (follower, post)
                                                        │ can't process
                                                        ▼
                                                  "post-created.dlt"

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
| [v4-reliable](https://github.com/gizawNahom/feed-fanout/tree/v4-reliable) | Dead-letter topic, retries with jittered backoff, transactional outbox, graceful shutdown, incremental rebalancing | Never lose or duplicate a post |
| v5-ops *(next)* | CI, logging, health checks, Docker, 3-broker Kafka | Production readiness |
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

# 3. Run the API, the outbox relay, and a fanout worker (one terminal each)
uv run uvicorn feed_fanout.api:app --reload
uv run python -m feed_fanout.relay
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

## The outbox

Saving a post and publishing its event are two writes to two systems. Done one after the other, a crash in between leaves a post that is never fanned out, or an event for a post that was never saved (the **dual-write problem**).

So the API doesn't talk to Kafka at all. It inserts the post **and** a row in the `outbox` table **in the same transaction**: both are committed or neither is. A separate relay process then:

1. locks a batch of outbox rows with `SELECT … FOR UPDATE SKIP LOCKED` (other relays skip them rather than wait or publish them twice),
2. publishes them and waits for the broker to acknowledge each one,
3. deletes the acknowledged rows and commits. Failed rows stay and are retried with backoff.

A crash between 2 and 3 republishes those events on restart: delivery is **at-least-once**, and the idempotent fanout makes duplicates harmless. Bonus: posting keeps working while Kafka is down; events just wait in the outbox. The outbox's row count is the publish backlog (look in Adminer).

*Alternatives:* at larger scale, change data capture (e.g. Debezium reading the Postgres WAL) replaces polling, and `LISTEN/NOTIFY` can wake the relay instead of a 0.5s poll.

## Failure handling

The worker commits an event's offset only once it has been fanned out **or** safely moved to the dead-letter topic. So every event ends up in exactly one of those two places.

| Failure | Example | What the worker does |
|---|---|---|
| **Permanent** | Malformed event, post doesn't exist | Publishes the event to **`post-created.dlt`** unchanged, with `dlt.*` headers (reason, error, original topic/partition/offset, time), then moves on |
| **Transient** | Database unreachable, connection lost, server restarting, deadlock, serialization failure | **Pauses consumption and retries the same event** with exponential backoff and full jitter (random wait up to 1s, 2s, 4s… capped at 30s), indefinitely |
| **Unexpected** | A bug, a statement timeout, anything not known to be transient | Retries 3 times, then dead-letters it with reason `unexpected` |

**Transient means *narrowly* transient.** Errors are classified by Postgres SQLSTATE, not exception class: a statement timeout is an `OperationalError` just like a refused connection, but it's about *this* event (say, a huge fanout) and would likely time out again. Retried forever, it would block its partition, so only connection errors (no server response, or class `08`), server restarts (`57P0x`), deadlocks and serialization failures (`40P01`, `40001`) count as transient.

**Why no retry topic?** Retry topics suit failures that hit *individual* events (say, one rate-limited API call). Our transient failure is the database being down, which is systemic: while it lasts, no event can succeed. Moving events to a retry topic would drain the whole backlog into it and break per-author ordering, for nothing. Pausing and retrying in place loses nothing and keeps order. While backing off, the worker keeps polling with its partitions paused, so Kafka doesn't evict it from the group for being too slow (`max.poll.interval.ms`).

### Inspecting and replaying dead letters

Browse them in Kafka UI (topic `post-created.dlt`, headers included), or use the replay tool. It runs as a **dry run by default**:

```bash
uv run python -m feed_fanout.replay_dlt
#   3 dead letter(s) in post-created.dlt: unexpected=2, malformed=1
#   would replay: partition 0 offset 4  key=alice  reason=unexpected  replays=0  failed_at=...
#       error: ValueError('bug')
```

Once the cause is fixed (say, the bug fix is deployed), send the events back to their original topic, where they're processed like any new event:

```bash
uv run python -m feed_fanout.replay_dlt --reason unexpected --execute
uv run python -m feed_fanout.replay_dlt --from-offset 3 --to-offset 7 --execute
```

- **Stateless.** It selects by offset range and reason and never commits anything, so a filtered run doesn't use up the dead letters it skipped. Replaying twice is harmless, since fanout is idempotent. The dead-letter topic is never modified.
- **No infinite loops.** Each replay increments a `dlt.replay_count` header that travels with the event. If a replayed event fails again, it returns to the dead-letter topic with its count (and fresh `dlt.*` headers, not duplicates). After `--max-replays` (default 3) the tool refuses to replay it again unless you pass `--force`.

## Shutdown and rebalancing

Deploys, `docker stop`, and Kubernetes stop a process with **SIGTERM**, then SIGKILL after a grace period. The worker and relay handle it:

- **Finish the current event, commit, then leave.** The worker calls `consumer.close()`, which tells Kafka it's leaving, so its partitions go to the other workers **immediately**, not after the ~45s session timeout. The relay finishes its batch and flushes.
- **Mid-retry shutdown.** A worker backing off during a database outage stops waiting at once and exits **without committing**: the event is re-delivered, not lost.
- **A second signal forces exit**, for when someone really means it.

Workers use Kafka's **new consumer rebalance protocol** (`group.protocol=consumer`, KIP-848, GA in Kafka 4.0). The broker assigns partitions incrementally, so a worker joining or leaving pauses only the partitions that move, not the whole group.

If a partition is reassigned between a worker fanning out an event and committing it, the commit is rejected. The worker logs it and moves on: the new owner re-processes the event from the last committed offset, and fanout is idempotent. While backing off, the worker resumes whatever it owns *after* the wait, since a rebalance may have changed it.

*Not done:* static membership (`group.instance.id`), which lets a worker restarted within the session timeout rejoin without any rebalance. It's worth it for rolling deploys once the services are deployed (v5).

## Tests

```bash
uv run pytest
```

Integration tests run against **real Postgres and Kafka** in throwaway containers ([testcontainers](https://testcontainers.com/)), using the same images as `docker-compose.yml`. Your dev containers and data are never touched. Each test gets empty tables and its own Kafka topic. The schema is built by running the real Alembic migrations, so the migrations are tested too. Docker must be running.

They cover the behavior that matters: followers receive posts and non-followers don't; replaying an event adds no duplicates; bad events land in the dead-letter topic with their context, and the worker moves on; dead letters can be replayed once fixed, without looping forever; a database outage is retried until it succeeds; SIGTERM exits promptly, commits finished work and leaves the group (tested against a real subprocess); a worker joining mid-stream loses nothing; feeds are newest-first; and pagination returns every post exactly once, even when timestamps tie.

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
- **Kafka outage:** stop Kafka (`docker compose stop kafka`) and create posts. The API still returns `201`, and the rows wait in `outbox`. Start Kafka and the relay drains them.
- **Idempotency:** rewind the consumer group and replay events. The worker logs `feeds_added=0`, and no feed gets a duplicate.
- **Database outage:** stop the worker, create a post, run `docker compose stop postgres`, and start the worker. It backs off (1s, 2s, 4s…). Run `docker compose start postgres` and the post is fanned out.
- **Graceful handover:** run two workers, then `kill <pid>` one of them (SIGTERM). It exits in under a second, and `kafka-consumer-groups.sh --describe --group fanout-workers --members` shows the survivor owning all partitions within a heartbeat or two.
- **Dead letters:** produce garbage to `post-created` with the console producer and watch it land in `post-created.dlt`.

## What's worth knowing

- **Thin events.** The Kafka event carries only `post_id`, `author_id`, and `created_at`. The post itself lives in Postgres, the single source of truth.
- **Fanout is one SQL statement.** `INSERT INTO feed_items ... SELECT FROM follows ... ON CONFLICT DO NOTHING`. Postgres reads the followers and writes the feed rows itself, without loading anything into Python.
- **At-least-once + idempotency.** The worker commits a Kafka offset only after the fanout transaction commits. If it crashes in between, the event is re-delivered, and the `(user_id, post_id)` primary key makes the re-run a no-op.
- **Keyset pagination.** Feeds page by `(created_at, post_id) < cursor` instead of `OFFSET`, so pages stay stable while new posts arrive, and deep pages stay fast.
- **Keyed partitioning across clients.** librdkafka hashes keys with CRC32 by default, while Java clients use murmur2. The producer sets `partitioner: murmur2_random` so a key always maps to the same partition.
- **Idempotent producer.** `enable.idempotence` prevents the producer's own retries from duplicating or reordering messages.

## Current limitations

- **Relay ordering is best-effort.** If one event fails to publish while later ones for the same author succeed, or several relays run at once, an author's events can reach Kafka out of order. Harmless here, since feeds are sorted by `created_at` and fanout is idempotent, but a consumer that needed strict per-author order would need a single relay and to stop at the first failure.
- **Single Kafka broker; services run outside Docker.** *(v5)*

## License

[MIT](LICENSE)
