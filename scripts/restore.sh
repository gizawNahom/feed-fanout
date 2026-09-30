#!/usr/bin/env bash
# Restore the database from its backups (pgBackRest), then bring the app back.
# The procedure in docs/runbooks/restore.md; scripts/restore_test.sh runs this exact script.
#
# Usage: scripts/restore.sh                              everything archived (e.g. the disk was lost)
#        scripts/restore.sh "2026-09-30 10:15:00+00"     the database as it was at that moment
#                                                        (e.g. just before a mistaken DELETE)
#
# Everything after the chosen moment is gone afterwards: take it from `pgbackrest info` and the
# logs, not a guess. Works on the stack started with docker-compose.backup.yml
# (COMPOSE_PROJECT_NAME picks which one).
set -euo pipefail
cd "$(dirname "$0")/.."

C=(docker compose -f docker-compose.yml -f docker-compose.backup.yml)
target=${1:-}
start=$(date +%s)

step() { echo; echo "=== $*"; }

step "saving the latest changes to the archive"
# Ship the WAL segment in progress, so the archive has every change up to now: a restore can
# only reach what was archived. (Skipped if Postgres is already down, e.g. the disk was lost.)
if "${C[@]}" exec -T backup pgbackrest --stanza=main check 2>/dev/null; then
    echo "  ok"
else
    echo "  Postgres or the scheduler is down: restoring from what was already archived"
fi

step "stopping everything that writes to the database"
"${C[@]}" stop api relay worker backup postgres

step "restoring ${target:-to the end of the archive}"
args=(--stanza=main --delta)  # --delta: keep files that already match the backup, rewrite the rest
if [[ -n "$target" ]]; then
    # Replay the archived WAL up to that moment, then open the database for writes.
    args+=(--type=time "--target=$target" --target-action=promote)
fi
"${C[@]}" run --rm --no-deps -T backup pgbackrest "${args[@]}" restore

step "starting Postgres (it replays the archived WAL first)"
"${C[@]}" up -d --wait --no-recreate postgres
restored=$(date +%s)

step "starting the app and the backup scheduler"
"${C[@]}" up -d --wait --no-recreate api relay worker backup

step "taking a fresh full backup (the restore started a new timeline)"
"${C[@]}" exec -T backup pgbackrest --stanza=main --type=full backup

step "restored: database back in $((restored - start))s, everything back in $(($(date +%s) - start))s"
