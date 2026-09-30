#!/usr/bin/env bash
# Backup and point-in-time restore test, through the same script an operator would run:
#
#   1. posts go in, a first full backup exists, WAL is archived continuously
#   2. a moment T is noted; after it, another post, then a disaster: DELETE FROM posts
#   3. scripts/restore.sh T  -> the database as it was at T
#   4. everything before T is back (posts and feeds), nothing after T is,
#      and the app works on the restored database: a new post reaches the feed
#
# Runs as its own compose project (own containers and volumes), so it never touches your
# dev stack's data, but it needs the same host ports: stop other stacks first (make down).
#
# Usage: scripts/restore_test.sh          (tears the test stack down afterwards)
#        KEEP=1 scripts/restore_test.sh   (leaves it running to poke at)
set -euo pipefail
cd "$(dirname "$0")/.."

export COMPOSE_PROJECT_NAME=feed-fanout-restore
C=(docker compose -f docker-compose.yml -f docker-compose.backup.yml)
API=http://localhost:8000
KEEP=${KEEP:-0}

step() { echo; echo "=== $*"; }

for port in 8000 5432 9092 8080 8081; do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
        echo "port $port is in use: stop other stacks first (make down)"
        exit 1
    fi
done

finish() {
    status=$?
    if [[ $status -ne 0 ]]; then
        echo; echo "=== diagnostics"
        "${C[@]}" ps || true
        "${C[@]}" logs --tail 40 postgres backup api || true
    fi
    if [[ "$KEEP" == "0" ]]; then "${C[@]}" down --volumes --remove-orphans; fi
    exit $status
}
trap finish EXIT

# wait_until <seconds> <description> <command...>: retry the command until it succeeds.
wait_until() {
    local timeout=$1 what=$2; shift 2
    for ((i = 0; i < timeout; i++)); do
        if "$@"; then return 0; fi
        sleep 1
    done
    echo "FAIL: timed out after ${timeout}s waiting for: $what"
    return 1
}

token() { "${C[@]}" run --rm --no-deps -T dev-keys python -m feed_fanout.devtools token "$1" 2>/dev/null; }
sql() { "${C[@]}" exec -T postgres psql -U feed -d feed -tAc "$1"; }
post() {
    curl -fsS -X POST "$API/posts" -H 'content-type: application/json' -H "$author_auth" \
        -d "{\"text\": \"$1\"}" | python3 -c 'import json, sys; print(json.load(sys.stdin)["id"])'
}
feed() { curl -fsS "$API/me/feed?limit=100" -H "$reader_auth"; }
in_feed() { feed | grep -q "$1"; }

step "starting the stack with backups (waits for the first full backup)"
"${C[@]}" up -d --build --wait --wait-timeout 300

author_auth="Authorization: Bearer $(token restore-author)"
reader_auth="Authorization: Bearer $(token restore-reader)"

step "before T: reader follows author, 3 posts reach the feed"
curl -fsS -X PUT "$API/me/following/restore-author" -H "$reader_auth"
before=()
for i in 1 2 3; do before+=("$(post "before T #$i")"); done
for id in "${before[@]}"; do wait_until 60 "post $id in the feed" in_feed "$id"; done
echo "  ok"

T=$(sql "SELECT now()")
step "T = $T"

step "after T: one more post, then the disaster"
after=$(post "after T")
wait_until 60 "the post after T in the feed" in_feed "$after"
sql "DELETE FROM posts" >/dev/null  # cascades to every feed
[[ "$(sql "SELECT count(*) FROM posts")" == "0" ]]
echo "  every post deleted"

step "restoring to T"
scripts/restore.sh "$T"

step "checking the restored database"
[[ "$(sql "SELECT count(*) FROM posts")" == "3" ]] || { echo "FAIL: expected exactly the 3 posts before T"; exit 1; }
f=$(feed)
for id in "${before[@]}"; do grep -q "$id" <<<"$f" || { echo "FAIL: $id (before T) missing from the feed"; exit 1; }; done
if grep -q "$after" <<<"$f"; then echo "FAIL: the post after T came back"; exit 1; fi
echo "  ok: the 3 posts before T are back, in the feed too; the one after T is not"

step "the app works on the restored database"
new=$(post "after the restore")
wait_until 60 "a new post in the feed" in_feed "$new"
echo "  ok: new posts fan out"

step "backups continue: a fresh full backup exists on the new timeline"
"${C[@]}" exec -T backup pgbackrest --stanza=main info | grep -E "status|full backup"

step "PASS: restored to T, nothing before it lost, nothing after it kept"
