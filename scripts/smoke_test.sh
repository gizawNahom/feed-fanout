#!/usr/bin/env bash
# End-to-end smoke test of the containerized stack:
#   docker compose up -> follow -> post -> the post reaches the follower's feed.
#
# Usage: scripts/smoke_test.sh            (leaves the stack running)
#        KEEP=0 scripts/smoke_test.sh     (tears it down afterwards, e.g. in CI)
set -euo pipefail
cd "$(dirname "$0")/.."

API=http://localhost:8000
KEEP=${KEEP:-1}

cleanup() {
    if [[ "$KEEP" == "0" ]]; then docker compose down --volumes --remove-orphans; fi
}
trap cleanup EXIT

echo "--- starting the stack (waits until every service is healthy)"
docker compose up -d --build --wait --wait-timeout 180

echo "--- health"
curl -fsS "$API/health/ready"; echo

# Unique names, so the test works against a stack that already has data.
run=$(date +%s)
author="smoke-author-$run"
reader="smoke-reader-$run"

echo "--- $reader follows $author, $author posts"
curl -fsS -X PUT "$API/users/$reader/following/$author"
post_id=$(curl -fsS -X POST "$API/posts" -H 'content-type: application/json' \
    -d "{\"author_id\": \"$author\", \"text\": \"smoke test $run\"}" \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["id"])')
echo "post_id=$post_id"

echo "--- waiting for fanout (API -> outbox -> relay -> Kafka -> worker -> feed)"
for _ in $(seq 1 60); do
    if curl -fsS "$API/users/$reader/feed" | grep -q "$post_id"; then
        echo "PASS: the post reached $reader's feed"
        exit 0
    fi
    sleep 1
done

echo "FAIL: the post did not reach the feed within 60s"
docker compose ps
docker compose logs --tail 50 api relay worker
exit 1
