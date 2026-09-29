#!/usr/bin/env bash
# Broker failover test against the 3-node Kafka cluster:
#
#   1. 1 broker killed  -> posts still reach followers' feeds
#   2. 2 brokers killed -> the API still accepts posts, but Kafka refuses writes it can't
#                          replicate (min.insync.replicas=2): the posts wait in the outbox
#   3. brokers back     -> the outbox drains and every post arrives. Nothing is lost.
#
# Runs as its own compose project (own containers and volumes), so it never touches your
# dev stack's data, but it needs the same host ports: stop other stacks first (make down).
#
# Usage: scripts/failover_test.sh          (tears the test stack down afterwards)
#        KEEP=1 scripts/failover_test.sh   (leaves it running to poke at)
set -euo pipefail
cd "$(dirname "$0")/.."

export COMPOSE_PROJECT_NAME=feed-fanout-failover
C=(docker compose -f docker-compose.yml -f docker-compose.cluster.yml)
API=http://localhost:8000
KEEP=${KEEP:-0}

step() { echo; echo "=== $*"; }

for port in 8000 5432 9092 9094 9096 8080 8081; do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
        echo "port $port is in use: stop other stacks first (make down)"
        exit 1
    fi
done

diagnostics() {
    echo; echo "=== diagnostics"
    "${C[@]}" ps || true
    "${C[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 \
        --describe --topic post-created || true
    "${C[@]}" logs --tail 40 relay worker || true
}
finish() {
    status=$?
    if [[ $status -ne 0 ]]; then diagnostics; fi
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

reader="failover-reader"
author="failover-author"
token() { "${C[@]}" run --rm --no-deps -T dev-keys python -m feed_fanout.devtools token "$1" 2>/dev/null; }
posted=()

post() {
    local id
    id=$(curl -fsS -X POST "$API/posts" -H 'content-type: application/json' -H "$author_auth" \
        -d "{\"text\": \"$1\"}" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["id"])')
    posted+=("$id")
    echo "  posted $id ($1)"
}
feed() { curl -fsS "$API/me/feed?limit=100" -H "$reader_auth"; }
all_in_feed() { local f; f=$(feed); for id in "${posted[@]}"; do grep -q "$id" <<<"$f" || return 1; done; }
outbox_size() { "${C[@]}" exec -T postgres psql -U feed -d feed -tAc "SELECT count(*) FROM outbox"; }
outbox_empty() { [[ "$(outbox_size)" == "0" ]]; }

step "starting the 3-broker stack"
"${C[@]}" up -d --build --wait --wait-timeout 300

step "topics are replicated 3x and need 2 in-sync replicas"
describe=$("${C[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 \
    --describe --topic post-created)
grep -q "ReplicationFactor: 3" <<<"$describe"
grep -q "min.insync.replicas=2" <<<"$describe"
echo "  ok"

step "baseline: $reader follows $author; a post reaches the feed"
author_auth="Authorization: Bearer $(token "$author")"
reader_auth="Authorization: Bearer $(token "$reader")"
curl -fsS -X PUT "$API/me/following/$author" -H "$reader_auth"
post "all brokers up"
wait_until 60 "baseline post in feed" all_in_feed
echo "  ok"

step "kill 1 broker (kafka-3): posts must still reach the feed"
"${C[@]}" kill kafka-3
for i in 1 2 3 4 5; do post "one broker down #$i"; done
wait_until 90 "posts in feed with one broker down" all_in_feed
echo "  ok: all ${#posted[@]} posts delivered"

step "kill a 2nd broker (kafka-2): posting still works, delivery must pause, nothing lost"
"${C[@]}" kill kafka-2
before=${#posted[@]}
for i in 1 2 3; do post "two brokers down #$i"; done
sleep 20
waiting=$(outbox_size)
echo "  outbox has $waiting event(s) waiting"
if [[ "$waiting" -lt 3 ]]; then
    echo "FAIL: expected the posts to wait in the outbox (Kafka should refuse under-replicated writes)"
    exit 1
fi
f=$(feed)
for id in "${posted[@]:$before}"; do
    if grep -q "$id" <<<"$f"; then
        echo "FAIL: $id was delivered with only one broker up"
        exit 1
    fi
done
echo "  ok: accepted by the API, held back from Kafka"

step "bring both brokers back: the outbox drains and every post arrives"
"${C[@]}" start kafka-2 kafka-3
wait_until 180 "outbox to drain" outbox_empty
wait_until 120 "every post in the feed" all_in_feed

step "PASS: ${#posted[@]} posts, 2 broker failures, 0 lost"
