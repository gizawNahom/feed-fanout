#!/usr/bin/env bash
# Run the load steps against the stack from loadtest/setup.sh, then write a report.
#
#   1. a short warm-up (not reported)
#   2. for each rate: STEP of constant load, then PAUSE for any backlog to drain
#   3. loadtest/report.py: one row per step, from k6 (what clients saw), Prometheus (what the
#      system did) and container stats (CPU, memory), and where the knee is
#
# Everything lands in loadtest/results/<time>/. Stop early with Ctrl-C: the steps already run
# stay, and `python3 loadtest/report.py <dir>` reports them.
#
# Usage: loadtest/run.sh
#        RATES="50 100 200 400 800" STEP=3m PAUSE=60 WARMUP=60s loadtest/run.sh   (the defaults)
set -euo pipefail
cd "$(dirname "$0")/.."

export COMPOSE_PROJECT_NAME=feed-fanout-load
C=(docker compose -f docker-compose.yml -f docker-compose.loadtest.yml --profile observability --profile loadtest)
RATES=${RATES:-50 100 200 400 800}
STEP=${STEP:-3m}
PAUSE=${PAUSE:-60}
WARMUP=${WARMUP:-60s}
USERS=${USERS:-100000}

[[ -s loadtest/tokens.json ]] || { echo "no tokens: run loadtest/setup.sh first"; exit 1; }
curl -fsS localhost:8000/health/ready >/dev/null || { echo "the API isn't ready: run loadtest/setup.sh first"; exit 1; }

name=$(date +%Y%m%d-%H%M%S)
out=loadtest/results/$name
mkdir -p "$out"

# What this result belongs to: the machine, the code, the limits.
{
    echo "date: $(date -Is)"
    echo "commit: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (uncommitted changes)')"
    echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs), $(nproc) threads"
    echo "memory (as Docker sees it): $(docker info --format '{{.MemTotal}}' | awk '{printf "%.1f GiB", $1/2^30}')"
    echo "kernel: $(uname -r)"
    echo "rates: $RATES; step: $STEP; pause: $PAUSE"
} > "$out/host.txt"

# CPU and memory of every container, every 5 s, for the whole run.
(
    while true; do
        docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' \
            | grep '^feed-fanout-load-' | sed "s/^/$(date +%s),/" || true
        sleep 5
    done
) > "$out/stats.csv" &
sampler=$!
trap 'kill $sampler 2>/dev/null || true' EXIT

k6() {
    "${C[@]}" run --rm --no-deps -T --user "$(id -u):$(id -g)" k6 run --quiet -e USERS="$USERS" "$@" /loadtest/feed.js
}

echo "=== warm-up: 20/s for $WARMUP (not reported)"
k6 -e RATE=20 -e DURATION="$WARMUP" > /dev/null

echo "rate,start,end" > "$out/steps.csv"
for rate in $RATES; do
    echo "=== $rate requests/s for $STEP"
    start=$(date +%s)
    k6 -e RATE="$rate" -e DURATION="$STEP" --summary-export "/results/$name/k6-$rate.json" | tail -n 30
    echo "$rate,$start,$(date +%s)" >> "$out/steps.csv"
    echo "  pausing ${PAUSE}s (any backlog drains)"
    sleep "$PAUSE"
done

python3 loadtest/report.py "$out" | tee "$out/report.md"
echo
echo "Results: $out"
