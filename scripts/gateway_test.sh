#!/usr/bin/env bash
# API gateway test: what Kong (gateway/kong.yml) must do, checked against the running stack with
# 2 API instances.
#
#   1. only the public API is routed: /metrics is not reachable through the gateway
#   2. requests are spread across both API instances
#   3. an oversized body is refused at the edge (413)
#   4. a burst beyond the global rate limit is refused at the edge (429, with a retry hint)
#
# Usage: scripts/gateway_test.sh          (tears the stack down afterwards)
#        KEEP=1 scripts/gateway_test.sh   (leaves it running)
set -euo pipefail
cd "$(dirname "$0")/.."

export COMPOSE_PROJECT_NAME=feed-fanout-gateway
GATEWAY=http://localhost:8000
KEEP=${KEEP:-0}

finish() {
    status=$?
    if [[ $status -ne 0 ]]; then docker compose ps || true; docker compose logs --tail 30 kong api || true; fi
    if [[ "$KEEP" == "0" ]]; then docker compose down --volumes --remove-orphans; fi
    exit $status
}
trap finish EXIT

status_of() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
expect() {  # expect <what> <expected> <actual>
    if [[ "$3" != "$2" ]]; then echo "FAIL: $1: expected $2, got $3"; exit 1; fi
    echo "  ok: $1 ($3)"
}
# Requests to /health/ready API instance N has counted, from its own metrics, through its debug
# port (Docker picks one from the range in docker-compose.yml: ask it which).
served_by() {
    local port
    port=$(docker compose port --index "$1" api 8000 | cut -d: -f2)
    curl -fsS "localhost:$port/metrics" | awk '/^feed_fanout_http_requests_total\{.*route="\/health\/ready"/ {s += $2} END {print s + 0}'
}

echo "=== starting the stack with 2 API instances behind the gateway"
docker compose up -d --build --wait --wait-timeout 300 --scale api=2

echo "=== 1. only the public API is routed"
expect "/metrics through the gateway" 404 "$(status_of $GATEWAY/metrics)"
expect "/me/feed without a token (routed; the API checks tokens)" 401 "$(status_of $GATEWAY/me/feed)"
expect "/health/ready" 200 "$(status_of $GATEWAY/health/ready)"

echo "=== 2. requests are spread across both API instances"
sleep 6  # Kong refreshes its DNS view of the instances every 5 s
before1=$(served_by 1); before2=$(served_by 2)
for _ in $(seq 40); do curl -fsS -o /dev/null $GATEWAY/health/ready; done
got1=$(( $(served_by 1) - before1 )); got2=$(( $(served_by 2) - before2 ))
echo "  40 requests: instance 1 served $got1, instance 2 served $got2"
if (( got1 < 10 || got2 < 10 )); then echo "FAIL: requests weren't spread across both instances"; exit 1; fi

echo "=== 3. an oversized body is refused at the edge"
expect "a 100 KB post" 413 "$(head -c 102400 /dev/zero | tr '\0' x | status_of -X POST $GATEWAY/posts \
    -H 'content-type: application/json' --data-binary @-)"

echo "=== 4. a burst beyond the global rate limit is refused at the edge"
codes=$(seq 600 | xargs -P 100 -I{} curl -s -o /dev/null -w '%{http_code}\n' $GATEWAY/health/ready | sort | uniq -c)
printf '  %s\n' "$codes"
grep -q " 429$" <<<"$codes" || { echo "FAIL: no request was refused with 429"; exit 1; }
if grep -vqE " (200|429)$" <<<"$codes"; then echo "FAIL: unexpected status codes"; exit 1; fi
sleep 1
expect "retry hint on a refusal" 1 "$(seq 300 | xargs -P 100 -I{} curl -s -D - -o /dev/null $GATEWAY/health/ready \
    | grep -ci '^retry-after' | awk '{print ($1 > 0)}')"

echo "PASS: routes, balancing, size limit and rate limit, all at the gateway"
