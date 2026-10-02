# feed-fanout

[![CI](https://github.com/gizawNahom/feed-fanout/actions/workflows/ci.yml/badge.svg)](https://github.com/gizawNahom/feed-fanout/actions/workflows/ci.yml)

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

GET /me/feed ───────► API ──► Postgres: feed_items ⨝ posts (newest first)
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
| [v5-ops](https://github.com/gizawNahom/feed-fanout/tree/v5-ops) | CI, logging, health checks, Docker, 3-broker Kafka | Production readiness |
| **v6-scale** *(in progress)* | Metrics, load testing, caching, celebrity handling | Measured performance |

## Quick start

Requires only Docker.

```bash
git clone https://github.com/gizawNahom/feed-fanout.git
cd feed-fanout
docker compose up -d --build --wait
```

That starts everything and waits until it's healthy: Kafka and Postgres, a one-shot service that creates the topics, one that runs the database migrations, then the API, the outbox relay, and a fanout worker. Everything runs in containers, as an unprivileged user, with JSON logs.

Try it:

```bash
ALICE="Authorization: Bearer $(make -s token AS=alice)"               # dev access tokens
BOB="Authorization: Bearer $(make -s token AS=bob)"

curl -X PUT localhost:8000/me/following/alice -H "$BOB"               # bob follows alice
curl -X POST localhost:8000/posts -H "$ALICE" \
  -H 'content-type: application/json' -d '{"text": "hello world"}'     # alice posts
curl localhost:8000/me/feed -H "$BOB"                                 # bob's feed
```

More:

```bash
docker compose up -d --scale worker=3      # more fanout workers (Kafka splits the partitions)
docker compose logs -f worker              # follow the workers' JSON logs
docker compose ps                          # status, including each service's health
docker compose down                        # stop (add --volumes to also delete the data)
scripts/smoke_test.sh                      # end-to-end check: post -> follower's feed
```

There's a `Makefile` with shortcuts for these (`make up`, `make smoke`, `make down`, `make help` for the list).

### Production-like Kafka: a 3-broker cluster

The default stack runs **one** Kafka broker, light enough for any laptop. An overlay turns it into a 3-node cluster with production durability settings:

```bash
make reset          # switching between 1 and 3 brokers needs fresh Kafka data
make up-cluster     # = docker compose -f docker-compose.yml -f docker-compose.cluster.yml up -d --build --wait
```

| Setting | Value | Effect |
|---|---|---|
| Replication factor | 3 | every partition is copied to all 3 brokers |
| `min.insync.replicas` | 2 | a write is acknowledged only once 2 brokers have it (producers use `acks=all`) |
| Unclean leader election | off | an out-of-date replica never becomes leader, so acknowledged data is never silently dropped |
| KRaft quorum | 3 controllers | the cluster keeps working when any one node fails |

**What that means:** with 1 broker down, everything keeps working. With 2 down, Kafka **refuses** writes it can't replicate instead of accepting them without a backup. It chooses consistency over availability. The rest of the system absorbs this: the API keeps accepting posts, they wait in the outbox, and the relay delivers them once a second broker is back.

`make failover` ([`scripts/failover_test.sh`](scripts/failover_test.sh)) proves it on an isolated copy of the stack: it kills one broker and checks that posts still arrive, kills a second and checks that posts are accepted but held back, then restarts both and checks that every post is delivered. It runs in CI on every push.

### Backups: point-in-time restore

Postgres is the only source of truth, so it's backed up continuously with [pgBackRest](https://pgbackrest.org), in another overlay:

```bash
make up-backup                              # the stack, with WAL archiving and daily full backups
make restore TO="2026-09-30 10:15:00+00"    # the database as it was at that moment
```

- **How:** Postgres archives its WAL (the log of every change) as it goes, at least every 60 seconds, and a scheduler takes a full backup daily and keeps 7. A restore takes the newest full backup before the target and replays WAL up to that exact moment. Backups are compressed and encrypted (AES-256).
- **RPO** (data you can lose): at most 60 s of writes if the machine is lost; nothing before the chosen moment for a planned restore, such as undoing a mistaken `DELETE`. **RTO:** about 10 s for the database in the test (small data); it grows with the database size and a day's WAL.
- **Tested on every push:** `make restore-test` ([`scripts/restore_test.sh`](scripts/restore_test.sh)) posts, notes a moment T, posts again, runs `DELETE FROM posts`, then restores to T through the same [`scripts/restore.sh`](scripts/restore.sh) an operator would use. It checks the posts before T are back (feeds too), the one after isn't, and new posts fan out on the restored database.
- **Monitored:** `WALArchivingFailing` and `BackupsStale` alerts, from `postgres_exporter` and `pgbackrest_exporter` (with the observability profile).
- **Kafka after a restore:** the database is now older than Kafka, and the outbox plus idempotent fanout make that safe: republished events add nothing, and events for posts lost by the restore go to the dead-letter topic. Details, and the full procedure, in the [restore runbook](docs/runbooks/restore.md).

An overlay rather than the default: with archiving on and nothing taking backups, unarchived WAL would pile up until the disk is full. In production, point the repository at object storage (S3) and take the passphrase from a secret store; or use managed Postgres, which does all of this for you.

### Load test

How much traffic the system sustains on one machine, and what gives out first: [k6](https://k6.io) at a fixed arrival rate against 100,000 seeded users with a power-law follower graph, every service under a fixed CPU and memory limit. Each step reports latency, the post-to-feed delay, consumer lag and resources, and where the knee is. See [loadtest/README.md](loadtest/README.md).

**Results so far:** the [baseline](docs/performance/2026-09-30-baseline/README.md), before any tuning. It shows a ceiling of ~95 requests/s set by the API alone (one core, ~7 ms of CPU per request), a collapse past it (the API runs out of file descriptors), and fanout-on-write holding up except for one celebrity post that blocked the single worker for 10+ seconds. Then [profiling the API](docs/performance/2026-10-02-api-overhead/README.md) found two thirds of its CPU in framework plumbing (thread handoffs, middleware machinery); two small changes cut CPU per request from 7.3 to 3.3 ms. And [overload](docs/performance/2026-10-02-overload/README.md) no longer collapses it: at 4× its capacity it serves 146 requests/s with no errors, turns the rest away with an immediate 503, and answers what it admits in under half a second.

```bash
make loadtest-setup && make loadtest && make loadtest-down
```

### Running the app outside Docker (for development)

Requires [uv](https://docs.astral.sh/uv/). Start only the infrastructure and setup services, then run the processes on your machine (they log readable text on a terminal):

```bash
docker compose up -d kafka postgres kafka-ui adminer kafka-topics migrate
cp .env.example .env                                         # ENVIRONMENT=dev + local settings
uv run python -m feed_fanout.devtools keys --dir .dev-keys   # a dev key pair (git-ignored)
uv run uvicorn feed_fanout.adapters.http:app --reload
uv run python -m feed_fanout.adapters.kafka.relay
uv run python -m feed_fanout.adapters.kafka.worker
```

Access tokens for this setup: `uv run python -m feed_fanout.devtools token alice --dir .dev-keys`.

## Architecture

Hexagonal (ports and adapters), kept light: plain Python `Protocol`s and constructors, no framework.

```
feed_fanout/
  domain/         the rules, pure: Post creation, follow and page-size rules, feed positions and cursors,
                  the posting limit
  application/    use cases (CreatePost, Follow, Unfollow, ReadFeed, FanOutPost) and the ports they
                  need (repositories, Outbox, PostingBuckets, UnitOfWork, Clock), as interfaces
  adapters/
    http.py       FastAPI: HTTP in -> use case -> HTTP out; domain rejections become 400/422/429
    postgres/     the ports in SQL; the single INSERT ... SELECT fanout lives here
    kafka/        the fanout worker, the outbox relay, dead letters and replay
  config, logs, metrics, health, retry, shutdown   shared technical services
```

- **The use cases know nothing about HTTP, SQL or Kafka.** "Save a post and its event atomically" is an application rule (one `UnitOfWork`); Postgres supplies the transaction.
- **The efficient parts stay efficient:** the fanout is still one `INSERT ... SELECT` that never loads followers into Python. It's simply the Postgres adapter's implementation of `FeedRepository.add_to_followers`.
- **Why it matters here:** the same behavior tests run on in-memory adapters in microseconds and on the real stack in seconds (see [Tests](#tests)).

## API

Every call acts as the user in its access token (`Authorization: Bearer …`); there's no user id in bodies or paths.

| Method | Path | Description |
|---|---|---|
| `POST` | `/posts` | Post `{text}` as yourself → `201` with the post, or `429` if you're [posting too fast](#posting-limits) |
| `PUT` | `/me/following/{followee}` | Follow (idempotent) → `204` |
| `DELETE` | `/me/following/{followee}` | Unfollow → `204` |
| `GET` | `/me/feed?limit=20&cursor=…` | Your feed (feeds are private), newest first. Pass `next_cursor` from the response to get older posts. |

Missing or invalid token → `401`. Health checks and `/metrics` need no token (they're for probes and Prometheus; in production, keep them off the public network).

### Authentication

The API doesn't manage users or passwords. An **identity provider** (Auth0, Keycloak, Cognito, ...) logs users in and signs **JWT access tokens** with its private key; the API only verifies them, with the provider's **public** keys from its JWKS URL. A public key can check a token but can't create one, so even a compromised API can't forge identities.

- **Every token is checked** for its signature, expiry, issuer, audience and subject. The algorithm is **pinned to RS256**, so the classic forgeries are refused: an unsigned token (`alg: none`), or an HMAC "signature" made with the public key.
- **Key rotation needs no deploy:** each token names its key (`kid`), and a key we haven't seen triggers a re-fetch of the JWKS, at most every 30 s so made-up key ids can't flood the provider.
- **Identity comes only from the token.** With no user id in any body or path, "acting as someone else" can't even be expressed: no per-route permission check to forget.
- **Configuration** (production): `AUTH_JWKS_URL`, `AUTH_ISSUER`, `AUTH_AUDIENCE` from your provider.
- **Local development** uses a stand-in: the `dev-keys` service makes a key pair on first start (never committed), the API trusts its public key, and `make token AS=alice` prints a token. That single, non-rotating key is **only accepted with `ENVIRONMENT=dev`**: configured anywhere else, the settings are rejected, and without a JWKS URL a production API refuses to start.

### Posting limits

Every post fans out to all of the author's followers, so one runaway client (or a stolen token) could flood feeds and the pipeline. Each user can post **10 times in a row, then 10 a minute** (`POSTS_BURST`, `POSTS_PER_MINUTE`); beyond that, `429 Too Many Requests` with a `Retry-After` header saying how many seconds until the next post is allowed.

- **A token bucket, kept as one timestamp:** when the user's bucket will be full again. Each post pushes it 6 s further; the bucket is empty when it's a minute ahead of now. No counters to reset, no background refill ([`domain/posting_limit.py`](feed_fanout/domain/posting_limit.py)).
- **Checked in the same transaction as the post**, with the user's bucket row locked (`SELECT ... FOR UPDATE`). So the limit holds across any number of API instances, a refused post leaves no trace, and an accepted one can't be half-counted. Concurrent posts by the same user take turns; other users aren't affected. A test fires 12 concurrent posts at a bucket with 5 tokens and checks exactly 5 succeed (without the lock, it fails).
- **In Postgres, not Redis:** it's already there, the check is one row per post, and posting is the low-volume path. A separate store would be worth it for limits on every request, like reads.
- **Scope:** posting only. Reads and follows are cheap and bounded; a per-IP limit in front of the API (at the load balancer) is the place for coarse abuse protection.

### Overload protection

Past its capacity, the API answers new requests at once with `503 Service Unavailable` and `Retry-After: 1`, instead of queuing them. The cap is the number of requests in flight (`API_MAX_IN_FLIGHT`, default 30): about what one API process really works on at once. Admitting more only builds a queue inside the API (at 100, admitted requests waited up to 2.5 s under overload).

- **Why:** in the [first load test](docs/performance/2026-09-30-baseline/README.md), past capacity, requests piled up (in flight = arrival rate × time each takes, Little's law), connections with them, until the process ran out of file descriptors and couldn't even reach its database. It served *less* the harder it was pushed. Turning the excess away cheaply keeps it serving at capacity; clients retry.
- **Health checks and `/metrics` are never turned away,** so probes and Prometheus can still see an overloaded API.
- **Visible:** turned-away requests are counted under the route `overloaded`, and feed the API error-rate alert.
- **More capacity is horizontal:** one API process uses at most one core (Python's GIL); run more instances behind a load balancer. Likewise, run **one fanout worker per partition** (3) in production, at least 2 for redundancy: a slow fanout (a celebrity's post) then holds up only its own partition. The dev stack runs 1 to stay light; `docker compose up --scale worker=3` runs 3.

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
uv run python -m feed_fanout.adapters.kafka.replay_dlt
#   3 dead letter(s) in post-created.dlt: unexpected=2, malformed=1
#   would replay: partition 0 offset 4  key=alice  reason=unexpected  replays=0  failed_at=...
#       error: ValueError('bug')
```

Once the cause is fixed (say, the bug fix is deployed), send the events back to their original topic, where they're processed like any new event:

```bash
uv run python -m feed_fanout.adapters.kafka.replay_dlt --reason unexpected --execute
uv run python -m feed_fanout.adapters.kafka.replay_dlt --from-offset 3 --to-offset 7 --execute
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

## Configuration

All settings come from environment variables (or a `.env` file; see [`.env.example`](.env.example)), with defaults that match `docker-compose.yml`:

| Variable | Default |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `DATABASE_URL` | **Required**, except with `ENVIRONMENT=dev`: then `postgresql+psycopg://feed:feed@localhost:5432/feed` |
| `POST_CREATED_TOPIC` | `post-created` (dead letters go to `<topic>.dlt`) |
| `FANOUT_GROUP_ID` | `fanout-workers` |
| `LOG_LEVEL` | `INFO` (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `LOG_FORMAT` | `auto`: readable text on a terminal, JSON otherwise (`json`, `text` to force) |
| `ENVIRONMENT` | `production`. `dev` allows the local database default and the dev signing key; nothing else does |
| `AUTH_JWKS_URL`, `AUTH_ISSUER`, `AUTH_AUDIENCE` | how to verify access tokens (see [Authentication](#authentication)) |
| `POSTS_BURST`, `POSTS_PER_MINUTE` | `10`, `10`: each user's [posting limit](#posting-limits) |
| `DB_POOL_SIZE` | `20`: database connections each process keeps open and reuses (instances × this must stay under Postgres's `max_connections`, 100) |
| `API_MAX_IN_FLIGHT` | `30`: requests the API handles at once; beyond, an immediate 503 ([overload protection](#overload-protection)) |
| `WORKER_HEALTH_PORT` | `8001` (`0` disables) |
| `RELAY_HEALTH_PORT` | `8002` (`0` disables) |

Settings are validated at startup ([pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)), so a typo like `LOG_LEVEL=VERBOSE` fails immediately with a clear error.

## Development

```bash
uv run pytest                    # tests (needs Docker)
uv run ruff check .              # lint
uv run ruff format .             # format
uv run mypy feed_fanout tests    # types (strict)
```

CI ([GitHub Actions](.github/workflows/ci.yml)) runs all of these on every push and pull request, with tests on Python 3.12 (the oldest supported) and 3.14, plus the end-to-end smoke test against the containerized stack, the broker failover test against the 3-node cluster, and the backup and point-in-time restore test.

A separate [security workflow](.github/workflows/security.yml) runs on every push and weekly: pip-audit on the locked dependencies, Trivy on the container image and for committed secrets. See [SECURITY.md](SECURITY.md) for what's in place and how to report a vulnerability.

## Tests

Written so each behavior is tested **once**, then proven at every level (the idea: [subsecond TDD](https://github.com/subsecondtdd/todo-subsecond)):

```
          ┌──────────────────────────────────────────────────────┐
  slow    │ infrastructure & fault tests                         │  commits, SIGTERM, rebalancing, dead letters,
          │                                                      │  outbox, logs, metrics, health (+ compose e2e, failover)
          ├──────────────────────────────────────────────────────┤
          │ acceptance scenarios              [full]             │  the real stack: HTTP, Postgres, Kafka
          │ repository contract tests         [postgres]         │
          ├──────────────────────────────────────────────────────┤
          │ acceptance scenarios              [memory]           │  same scenarios, in-memory adapters
          │ repository contract tests         [memory]           │  keeps the fakes honest
  fast    │ unit tests                                           │  domain rules at their exact boundaries
          └──────────────────────────────────────────────────────┘
```

- **Acceptance scenarios** (`tests/acceptance/`) are written against a small contract (`follow`, `unfollow`, `post`, `feed`, `settle`, `time_passes`) in the users' language, with no HTTP, SQL or Kafka. Each runs on two **assemblies**: `memory` (the real use cases with in-memory adapters, ~160 µs per scenario, the whole set in ~8 ms) and `full` (the real stack, seconds). Same test, both levels.
- **Contract tests** (`tests/contracts/`) run the same checks against the Postgres repositories and the in-memory fakes: ordering, tie-breaking, idempotency, even the unknown-post quirk; posting buckets, plus concurrent posts that only Postgres can show. The fast tests are only as trustworthy as the fake; these catch drift (dropping the fake's tie-break fails exactly the two `[memory]` tie tests).
- **Unit tests** (`tests/unit/`) pin the domain rules at their exact boundaries, and the cursor format.
- **Infrastructure tests** (`tests/test_*.py`) cover what the contract can't see: Kafka commit semantics, dead-letter headers and replay, SKIP LOCKED, SIGTERM against a real subprocess, rebalancing, metrics and logs. Real Postgres and Kafka in throwaway containers ([testcontainers](https://testcontainers.com/)), the schema built by the real migrations; your dev data is never touched.

```bash
make test-fast    # memory scenarios: milliseconds, no Docker
make test         # everything (needs Docker)
make coverage     # all tests under coverage; fails below the floor (90%, measured 92%)
make mutation     # mutation testing on the core
```

**Mutation testing** ([mutmut](https://github.com/boxed/mutmut)) changes `domain/` and `application/` in small ways (`1 <= len` becomes `2 <= len`, a message becomes `None`, ...) and checks the fast tests notice each change: ~130 mutants in ~20 s, **100% detected**, enforced in CI. The first run found 18 gaps (untested boundaries, unchecked error messages) and one real performance risk that no behavior test could see: `ReadFeed` fetching the whole feed instead of one page plus one row, which would behave identically but load a user's entire history from Postgres on every request.

## Observability

| What | Where |
|---|---|
| API docs, with try-it-out | http://localhost:8000/docs |
| Kafka UI: messages, partitions, consumer lag | http://localhost:8080 |
| Adminer: browse `posts`, `follows`, `feed_items` | http://localhost:8081 (System: PostgreSQL, Server: `postgres`, user/password/database: `feed`) |
| Health checks | API http://localhost:8000/health/ready · worker http://localhost:8001/health/ready · relay http://localhost:8002/health/ready |
| Metrics dashboard (Grafana) | http://localhost:3000, with `make up-observability` |
| Metrics and alerts (Prometheus) | http://localhost:9090, alerts at http://localhost:9090/alerts |
| Logs | Structured JSON lines; searchable in Grafana via Loki (see below) |

### Metrics

Every process exposes Prometheus metrics at `/metrics` (API on :8000, worker on :8001, relay on :8002). Prometheus and a provisioned Grafana dashboard run in an optional Compose profile, so the default stack stays light:

```bash
make up-observability     # = docker compose --profile observability up -d --build --wait  (Prometheus, Grafana, Loki, Alloy)
```

| Metric | Why it matters |
|---|---|
| `feed_fanout_post_to_feed_seconds` | **What users experience:** from a post's creation until it's in its followers' feeds |
| `feed_fanout_http_requests_total`, `…_request_duration_seconds` | API traffic, errors and latency per route |
| `feed_fanout_worker_events_total{outcome}` | Fanned out, dead-lettered, or abandoned |
| `feed_fanout_worker_fanout_duration_seconds`, `…_feed_rows_written` | Cost of each fanout; the tail of rows-per-post is where celebrities show up |
| `feed_fanout_consumer_group_lag{partition}` | Are the workers keeping up? Measured from the broker (by the relay), not by the workers themselves: a worker stuck on one slow event can't report its own lag, but stops committing, so this still grows |
| `feed_fanout_outbox_backlog`, `…_oldest_age_seconds` | Is publishing keeping up? The oldest event's age matters more than the count |
| `feed_fanout_worker_retries_total{kind}`, `feed_fanout_dead_letters_total{reason}` | Problems that would otherwise be quiet |

- **Labels are low-cardinality only:** routes are recorded as templates (`/me/following/{followee_id}`), never raw paths, and nothing is labelled by user or post. Every distinct label value is a new time series; per-post detail lives in the logs.
- **Every worker is scraped,** including ones added with `--scale worker=N`: Prometheus discovers them through Compose's DNS.

**SLO: 99% of posts reach followers' feeds within 2 seconds,** set from the [load test measurements](docs/performance/): p99 about 1 s under normal load, and under 2 s even while the API was overloaded. Instead of alerting on any slow post, the alert fires when the error budget (the 1% allowed to be slower) is burning fast: over 14.4% slow, over both the last 5 minutes and the last hour. That catches real problems quickly and ignores brief spikes.

**Alerts** ([`observability/prometheus/rules.yml`](observability/prometheus/rules.yml)): the SLO burn; consumer lag high; an outbox event stuck for over a minute; events dead-lettered; API error rate; a target down; WAL archiving failing, and backups stale (with the backup overlay). They're **unit-tested** with `promtool test rules` (in CI, and `make rules`), which feeds synthetic time series through the rules and checks exactly when each alert fires, including that a 5-minute spike doesn't page anyone. (They show in Prometheus's Alerts page; routing them to Slack or a pager would be Alertmanager's job.)

### Logs

Every process logs through [structlog](https://www.structlog.org/): fields are keywords, not text inside the message. On a terminal you get readable text; anywhere else (containers, CI, files) one JSON object per line, ready for Loki, Datadog, CloudWatch & co.:

```json
{"msg": "post created", "post_id": "18822c67-…", "author_id": "alice", "logger": "feed_fanout.adapters.http", "level": "info", "ts": "2026-09-28T13:57:58.451Z"}
{"msg": "fanned out", "feeds_added": 3, "post_id": "18822c67-…", "author_id": "alice", "partition": 0, "offset": 29, "logger": "feed_fanout.adapters.kafka.worker", "level": "info", "ts": "2026-09-28T13:57:59.171Z"}
```

- **Follow one post across processes:** filter on `post_id` instead of grepping text. The worker binds the event's `post_id`, `author_id`, `partition`, and `offset` once per message, so *every* line while handling it (retries, dead-lettering) carries them automatically, and they're unbound afterwards so they never leak into the next message's lines.
- **One format per process:** uvicorn's and other libraries' logs go through the same pipeline.
- **No access log** (one line per HTTP request) in the containers: formatting one per request cost the API real CPU under load, and the metrics already count every request by route, status and latency. Running uvicorn by hand (`uv run uvicorn ...`) keeps it, for debugging.
- **Exceptions are one structured field** (`exception: [{exc_type, exc_value, frames…}]`), not a multi-line traceback that log collectors split into separate entries.

**Centralized in Loki** (with `make up-observability`). [Grafana Alloy](https://grafana.com/docs/alloy/) discovers the containers and ships their stdout to [Loki](https://grafana.com/oss/loki/); the apps know nothing about it. The same Grafana shows metrics and logs, and the dashboard's **Logs** row has a `post_id` box: paste an id and see every line about that post from the API and the workers, including containers that have since been replaced. Or query in Grafana's Explore:

```logql
{service="worker"} | json | post_id="18822c67-…"              # one post, in the workers
{project="feed-fanout", level=~"warning|error"} | json         # every warning and error
sum by (service) (count_over_time({project="feed-fanout"}[5m])) # log volume per service
```

- **Few labels, by design:** streams are labelled only by `service`, `container`, `project`, and `level`. Loki is fast because it indexes little; `post_id` stays in the line and is filtered at query time with `| json`. A `post_id` label would create a stream per post and grind Loki down, the same cardinality rule as for metrics.
- **3-day retention**, so a laptop's disk doesn't fill up. Docker's own copy of each container's output is capped too (3 files of 10 MB), so it can't grow without bound either: Loki is where history lives.
- **Alloy reads the Docker socket** (read-only) to find containers: normal for local development. In production, logs come from the platform's agent instead (e.g. a Kubernetes DaemonSet reading node log files).

### Health checks

Every process answers two probes, the way Docker and Kubernetes expect:

| Probe | Question | On failure, the orchestrator… |
|---|---|---|
| `GET /health/live` | Is the process stuck, so that restarting it would help? | restarts it |
| `GET /health/ready` | Should it be doing work right now? | stops routing work to it (no restart) |

- **API:** `live` always answers `200`. `ready` runs `SELECT 1` and returns `503`, with the database error, if Postgres can't be reached.
- **Worker and relay:** a small HTTP server on ports 8001/8002. `live` means the main loop is still making progress; `ready` means it's running and not shutting down. The response carries details for whoever is debugging:

```json
{"status": "ok", "live": true, "ready": true, "state": "backing_off",
 "consecutive_failures": 4, "last_loop_seconds_ago": 0.3, "last_success_seconds_ago": 42.1,
 "assigned_partitions": 2}
```

**Liveness never depends on other services.** If Postgres goes down, restarting every API instance and worker can't fix it; it only adds a restart storm on top of the outage. So a worker waiting out an outage (`state: backing_off`) is still *live*. Likewise, consumer lag isn't a health signal: a slow worker is alive, and restarting it only makes it slower. Lag belongs in metrics and alerts (v6).

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

## Production readiness

[docs/production-readiness.md](docs/production-readiness.md) lists what "production ready" means here, claim by claim (behavior, reliability, data, security, capacity, operability), each with its evidence: a test, a CI job, a measurement or a document. Operators have [runbooks](docs/runbooks/) for every paging alert and for restoring the database.

## Known limitations and next steps

What this deliberately doesn't do yet, and why that's acceptable for now:

- **Celebrities still cost a lot to fan out.** A post by an account with ~20,000 followers took over 10 s to write into every feed, holding up its partition meanwhile. One worker per partition limits the damage to a third of posts; the fix is **hybrid fanout**: ordinary authors fanned out on write, very large accounts merged in when a feed is read. It's the main risk to the 2 s SLO.
- **Excess load is turned away inside the API,** at ~1.8 ms of CPU per refused request. In production, refuse it at the load balancer or reverse proxy, before Python, where it's much cheaper.
- **Capacity is measured for one API process on one core** (~146 requests/s served under overload). Scaling out is horizontal (more instances behind a load balancer) and not measured here.
- **Backups are tested against a local repository,** not real object storage. The S3 settings are documented, not exercised.
- **The Postgres image with pgBackRest isn't vulnerability-scanned in CI** (the app image is). Scanned by hand (2026-10-02), it has 22 fixable HIGH/CRITICAL findings, all in `gosu`'s Go standard library: the official Postgres image uses `gosu` only to switch from root to the postgres user at startup, and they can only be fixed upstream. A blocking scan would keep CI red with nothing to fix here.
- **Monitoring gaps:** counters reset on restart can hide activity until the next scrape; there's no end-to-end test that alerts actually reach someone (routing them is Alertmanager's job, not set up).
- **Relay ordering is best-effort.** If one event fails to publish while later ones for the same author succeed, or several relays run at once, an author's events can reach Kafka out of order. Harmless here, since feeds are sorted by `created_at` and fanout is idempotent, but a consumer that needed strict per-author order would need a single relay and to stop at the first failure.

## License

[MIT](LICENSE)
