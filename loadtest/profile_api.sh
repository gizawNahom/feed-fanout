#!/usr/bin/env bash
# Profile the API while it serves load: where does its CPU time go?
#
# py-spy samples the running API's stacks from outside (another container sharing its process
# namespace): nothing is installed in, or restarted, in the API. Only threads holding Python's
# GIL are sampled, so the profile is CPU work, not threads waiting for a request or the database.
#
# Run it while a load runs, e.g.  terminal 1: RATES=50 STEP=2m make loadtest
#                                 terminal 2: make profile-api
#
# Writes loadtest/results/profile-<time>.speedscope.json (open it at https://www.speedscope.app)
# and a summary next to it (loadtest/profile_summary.py).
#
# Usage: loadtest/profile_api.sh
#        DURATION=60 RATE=100 TARGET=feed-fanout-load-api-1 loadtest/profile_api.sh   (the defaults)
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${DURATION:-60}
RATE=${RATE:-100}  # samples per second
TARGET=${TARGET:-feed-fanout-load-api-1}
PY_SPY=0.4.2

docker inspect -f '{{.State.Running}}' "$TARGET" 2>/dev/null | grep -q true \
    || { echo "$TARGET isn't running: start the load test stack first (make loadtest-setup)"; exit 1; }

mkdir -p loadtest/results
name="profile-$(date +%Y%m%d-%H%M%S)"

echo "=== profiling $TARGET for ${DURATION}s (${RATE} samples/s)"
# --pid=container: shares the API's process namespace (its uvicorn is PID 1 there).
# SYS_PTRACE lets py-spy read another process's memory; only this short-lived container has it,
# and Docker only grants it to root there. The profile is handed back to you at the end.
docker run --rm --pid="container:$TARGET" --cap-add SYS_PTRACE -v "$PWD/loadtest/results:/out" \
    python:3.14.7-slim-trixie sh -c "
        pip install --quiet --root-user-action=ignore py-spy==$PY_SPY >/dev/null &&
        py-spy record --pid 1 --gil --duration $DURATION --rate $RATE \
            --format speedscope --output /out/$name.speedscope.json &&
        chown $(id -u):$(id -g) /out/$name.speedscope.json"

python3 loadtest/profile_summary.py "loadtest/results/$name.speedscope.json" | tee "loadtest/results/$name.md"
echo
echo "Profile: loadtest/results/$name.speedscope.json (interactive: https://www.speedscope.app)"
echo "Summary: loadtest/results/$name.md"
