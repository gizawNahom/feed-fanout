#!/usr/bin/env bash
# Prepare a load test: start the stack with its resource limits, seed the data, make tokens.
# Its own compose project (feed-fanout-load): your dev stack's data is never touched, but it
# needs the same host ports, so stop other stacks first (make down).
#
# Usage: loadtest/setup.sh
#        USERS=100000 SKEW=2.13 POSTS_PER_USER=5 EVERY=10 loadtest/setup.sh   (the defaults)
set -euo pipefail
cd "$(dirname "$0")/.."

export COMPOSE_PROJECT_NAME=feed-fanout-load
# Without the loadtest profile: k6 only runs on demand (loadtest/run.sh).
C=(docker compose -f docker-compose.yml -f docker-compose.loadtest.yml --profile observability)
USERS=${USERS:-100000}
SKEW=${SKEW:-2.13}
POSTS_PER_USER=${POSTS_PER_USER:-5}
EVERY=${EVERY:-10}

for port in 8000 5432 9092 9090 3000; do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
        if ! "${C[@]}" ps --status running --quiet | grep -q .; then
            echo "port $port is in use: stop other stacks first (make down)"
            exit 1
        fi
    fi
done

mkdir -p loadtest/results  # made by you, so k6 (running as you) can write into it

echo "=== starting the stack (with its resource limits and observability)"
"${C[@]}" up -d --build --wait --wait-timeout 300

echo "=== seeding $USERS users (follow graph skew $SKEW, $POSTS_PER_USER posts each): a few minutes"
"${C[@]}" exec -T postgres psql -U feed -d feed -v ON_ERROR_STOP=1 \
    -v users="$USERS" -v skew="$SKEW" -v posts_per_user="$POSTS_PER_USER" < loadtest/seed.sql

echo "=== access tokens for every ${EVERY}th user"
"${C[@]}" run --rm --no-deps -T dev-keys \
    python -m feed_fanout.devtools tokens --users "$USERS" --every "$EVERY" > loadtest/tokens.json
echo "  $(python3 -c 'import json; print(len(json.load(open("loadtest/tokens.json"))))') tokens in loadtest/tokens.json"

echo
echo "Ready. Run the load steps with: make loadtest   (Grafana: http://localhost:3000)"
