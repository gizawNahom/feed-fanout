# Runbook: restore the database

Postgres is the only source of truth (posts, follows, feeds, the outbox), so this is the
procedure that recovers the product. It is tested on every push: `scripts/restore_test.sh`
runs the same `scripts/restore.sh` described here, against a real stack.

**Guarantees** (with [`docker-compose.backup.yml`](../../docker-compose.backup.yml)):

- **RPO, data you can lose:** at most 60 s of writes (`archive_timeout`), if the database
  machine is lost outright. For a planned restore (a mistake, not a lost disk), nothing is
  lost before the moment you choose.
- **RTO, time to restore:** the restore test measures it, about 10 s for the database and
  30 s for everything on its small dataset. It grows with the database: a restore copies the
  newest full backup before the target, then replays up to 24 h of WAL (backups are daily).
  Measure it on a production-sized copy before promising a number.
- **Window:** any moment from the oldest kept full backup (7 are kept) until now.

## 1. Decide what you're restoring to

| Situation | Restore to |
|---|---|
| The database's disk or volume is lost or corrupt | The end of the archive: `make restore` |
| A mistake: a bad `DELETE`/`UPDATE`, a buggy release writing bad data | The moment just before it: `make restore TO="…"` |

**Everything after the chosen moment is gone,** including legitimate posts and follows made
since. So:

- **Find the moment from evidence,** not memory: the API's JSON logs (every post, follow and
  unfollow is logged with a timestamp), the deploy history, or Postgres's own log. Pick a time just before the
  first bad write, with a timezone, e.g. `2026-09-30 10:15:00+00`.
- **Check it's in range:** `docker compose -f docker-compose.yml -f docker-compose.backup.yml
  exec backup pgbackrest --stanza=main info` lists the backups; the oldest one's start time is
  the earliest moment you can restore to.
- **If what happened after that moment matters,** save it first, while the database is still
  up (for example `pg_dump --table=posts` of the newer rows), to re-enter it afterwards.
- **If a migration is the problem:** restoring to before it isn't enough on its own. When the
  app comes back, the `migrate` service applies every migration in the current image again.
  Deploy the previous version (without the migration) first, then restore.

## 2. Restore

```bash
make restore TO="2026-09-30 10:15:00+00"   # or: make restore   (to the end of the archive)
```

[`scripts/restore.sh`](../../scripts/restore.sh) then:

1. **Archives the latest changes** (`pgbackrest check`): a restore can only reach what's in the
   archive. Skipped if Postgres is already down.
2. **Stops everything that writes:** the API, relay, workers, the backup scheduler, Postgres.
   The API is down from here until step 5 (clients get connection errors).
3. **Restores** the data directory from the right full backup, with `--delta` (only files that
   differ are rewritten).
4. **Starts Postgres,** which replays the archived WAL up to the target, then opens for writes
   on a new timeline.
5. **Starts the app** (migrations re-run, see above) and the backup scheduler.
6. **Takes a fresh full backup:** the next restore should start from after this one.

## 3. Check

- `curl localhost:8000/health/ready` answers `ok`.
- The data looks as expected at the target: spot-check the rows the incident touched, e.g.
  `SELECT count(*), max(created_at) FROM posts` (the newest post should be before the target).
- A new post reaches a follower's feed: follow and post with two dev tokens (`make token AS=…`),
  then read the feed. (Not `make smoke`: it restarts the stack without the backup overlay.)

## 4. What to expect afterwards

The database is now older than Kafka. The outbox and idempotent fanout make that safe:

- **Events for posts made after the target** are still in Kafka. Workers that hadn't processed
  them yet find the post missing and send them to the dead-letter topic with reason
  `unknown_post` (alert: `EventsDeadLettered`; an author without followers just gets nothing
  fanned out). That's expected: those posts were lost by the restore. **Don't
  replay them**, they'd just dead-letter again.
- **Outbox events published just before the target** may be restored and published a second
  time. Fanout is idempotent (`ON CONFLICT DO NOTHING`), so feeds get no duplicates.
- **Feeds match posts exactly:** both come from the same consistent point in time.
- **Consumer offsets are untouched** (they live in Kafka), so workers carry on from where they
  were, with no reprocessing of old events.

## In production

- **Keep the repository off the database machine** (S3 or similar: see the `PGBACKREST_REPO1_*`
  variables in `docker-compose.backup.yml`), and the encryption passphrase in a secret store,
  apart from the backups. Losing the passphrase makes every backup unreadable.
- **Restore real backups regularly,** not only in CI: once a month, restore production's
  backups to a scratch instance and run the checks above. It proves the backups are good and
  gives a real RTO.
- **Alerts:** `WALArchivingFailing` (page: the RPO no longer holds, and the disk will fill) and
  `BackupsStale` (ticket: no full backup in 26 h). Both need the observability profile.
- **Managed Postgres** (RDS, Cloud SQL, ...) does all of this itself: continuous archiving,
  point-in-time restore from the console. Then this runbook reduces to sections 1, 3 and 4.
