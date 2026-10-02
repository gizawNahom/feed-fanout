# Runbooks: alerts

One section per alert that pages: what it means, what users feel, what to check, what to do.
Alert rules: [`observability/prometheus/rules.yml`](../../observability/prometheus/rules.yml).
Dashboard: Grafana, *feed-fanout*. Commands are for the compose stack; in production, run the
same thing through your platform (a pod exec instead of `docker compose exec`).

A shorthand used below:

```bash
C="docker compose"   # plus your -f overlays, if the stack was started with them
```

## PostToFeedSLOBurn

**Means:** more than 14.4% of posts took over 2 s to reach followers' feeds, over both the last
5 minutes and the last hour. At that rate the month's error budget (1% of posts may be slower)
is gone in about two days.
**Users feel:** new posts show up late in their friends' feeds. Nothing is lost.

**Check:** on the dashboard, *Post-to-feed latency*, then find where posts wait:

| Panel | Points to |
|---|---|
| *Consumer lag* growing | the workers: see [ConsumerLagHigh](#consumerlaghigh) |
| *Outbox* age growing | publishing to Kafka: see [OutboxStuck](#outboxstuck) |
| *Fanout duration* tail high, *Feed rows written* tail high | a celebrity's post: one fanout to tens of thousands of followers holds up its partition |
| *API p99* high | posts are slow to be accepted: see [APIErrorRateHigh](#apierrorratehigh) |

**Do:** fix whichever stage is slow (sections below). A celebrity's fanout clears by itself;
if they're frequent, that's the [known limitation](../../README.md#known-limitations-and-next-steps)
hybrid fanout addresses.

## ConsumerLagHigh

**Means:** over 1,000 events have waited for the fanout workers for 5 minutes. Measured from the
broker, so it counts for stuck or dead workers too.
**Users feel:** posts reach feeds late, increasingly.

**Check:**
```bash
$C ps worker                                   # running and healthy? (healthy = ready and live)
$C logs --tail 100 worker                      # retrying? ("retrying" lines, database errors)
$C exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
    --describe --group fanout-workers           # lag per partition, and who owns it
```

**Do:**
- **Lag on one partition only:** a slow event there, usually a celebrity's fanout. Wait it out
  if lag is falling; check *Fanout duration*.
- **Workers retrying database errors:** it's the database. Fix that; the workers resume by
  themselves and lose nothing.
- **A worker unhealthy or missing:** restart it (`$C restart worker`). Its partitions are
  reprocessed from the last commit, which is safe: fanout is idempotent.
- **All healthy, just behind:** add workers, up to the partition count (3):
  `$C up -d --scale worker=3`.

## OutboxStuck

**Means:** a post has been waiting in the outbox for over a minute: saved, but not yet published
to Kafka. The relay is running (otherwise TargetDown fires instead).
**Users feel:** their posts are accepted but don't reach anyone. Nothing is lost: the outbox
keeps them until Kafka takes them.

**Check:**
```bash
$C logs --tail 100 relay                       # publish errors: which ones?
$C exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 \
    --describe --topic post-created             # leaders, in-sync replicas
```

**Do:** it's almost always Kafka refusing writes. On the 3-broker cluster, writes need 2 in-sync
replicas: with 2 brokers down, Kafka refuses them on purpose. Bring the brokers back; the relay
publishes the backlog by itself, in order. Nothing else to do.

## APIErrorRateHigh

**Means:** more than 5% of API requests returned a 5xx over 5 minutes.
**Users feel:** failed requests. Which kind decides what to do.

**Check:** on the dashboard, *API errors (5xx)* by route.

**Do:**
- **Route `overloaded`:** the API is at capacity and turning the excess away with 503 (overload
  protection, working as intended). Add API instances, and find out where the traffic comes from.
  In production, refuse excess load at the load balancer before it reaches Python: cheaper.
- **Any other route:** real errors. Search the logs (Grafana, Loki):
  `{service="api", level="error"} | json`. Database unreachable? Check
  `curl localhost:8000/health/ready` and Postgres. A bad deploy? Roll it back.

## TargetDown

**Means:** Prometheus has been unable to scrape a service for a minute: it's down, restarting
in a loop, or unreachable.
**Users feel:** depends on which one: the API (requests fail), the relay (posts aren't published),
a worker (fanout slows, other workers take over its partitions).

**Check:**
```bash
$C ps                                          # which one, and its status
$C logs --tail 100 <service>                   # why it stopped
```

**Do:** restart it (`$C up -d <service>`). If it crashes again, the logs say why: a config
error fails at startup with a clear message (settings are validated), a lost dependency shows as
connection errors. All three processes recover their work by themselves after a restart.

## WALArchivingFailing

**Means:** Postgres has failed to archive its WAL for 10 minutes, without one success.
**Users feel:** nothing yet. But recent changes aren't backed up (the 60-second RPO no longer
holds), and the WAL Postgres can't archive piles up on its disk until it's full, which **stops
the database**.

**Check:**
```bash
$C logs --tail 50 postgres | grep -i archive   # archive-push errors
$C exec backup pgbackrest --stanza=main check  # the backup repository, end to end
```

**Do:** the usual causes are the backup repository (full, unreachable, or credentials changed:
in production, the S3 bucket and its keys) or the encryption passphrase. Fix it, then confirm with
`pgbackrest check`: archiving catches up by itself. If the disk is close to full, fixing archiving
is the only safe way to free it: never delete WAL files by hand. See the
[restore runbook](restore.md) for the backup setup.

## Tickets (no page)

- **EventsDeadLettered:** some events couldn't be processed. Look at them:
  `uv run python -m feed_fanout.adapters.kafka.replay_dlt` (a dry run: lists them, with their
  reason). Fix the cause, then replay: `... --reason <reason> --execute`. `unknown_post` after
  a database restore is expected: [don't replay those](restore.md#4-what-to-expect-afterwards).
- **BackupsStale:** no full backup in 26 hours, or the repository can't be read. Restores still
  work, from older backups plus WAL, just slower. Check `$C logs backup`, then take one by hand:
  `make backup`.
