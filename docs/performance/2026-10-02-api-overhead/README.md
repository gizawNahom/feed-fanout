# API overhead: profiled and fixed, 2026-10-02

The [baseline](../2026-09-30-baseline/README.md) found the API was the bottleneck at ~7 ms of
CPU per request, and asked where that time goes. A profile under load answered it: mostly
framework plumbing our code set off, not our code. Two small changes halved the CPU per request.

## Summary

- **CPU per request: 7.3 ms → 3.3 ms (−55%)** at 50 requests/s, same resource budget.
- **Where it went:** about two thirds of the API's CPU was moving work between the event loop and
  worker threads, and the machinery of two HTTP middlewares. Our own code was 1.7%.
- **The fix:** the middlewares rewritten as plain ASGI middleware, and the dependencies that only
  return an object made `async`, so they no longer cost a thread handoff each. Behavior unchanged;
  all 270 tests pass.
- Latency fell from hundreds of milliseconds to tens (feed p99 764 ms → 13 ms), though not all of
  that can be credited to this fix (see [Caveats](#caveats)).

## What the profile showed

[py-spy](https://github.com/benfred/py-spy) sampled the API's stacks 100 times a second for 60 s,
at 50 requests/s, counting only threads doing Python work ([profile-before.md](profile-before.md)):

| Where the CPU went (self time) | Share |
|---|---|
| anyio, asyncio, threading: handing work between the event loop and worker threads | ~36% |
| starlette, fastapi: middleware, routing, resolving dependencies | ~25% |
| sqlalchemy, psycopg: the database work itself | ~17% |
| Everything else: uvicorn ~4%, pydantic ~3%, logging ~3%, our own code 1.7%, ... | ~22% |

Two things in our code set that machinery off:

1. **Two middlewares declared with `@app.middleware("http")`** (request metrics, and the
   `nosniff` header). Starlette implements that with `BaseHTTPMiddleware`, which starts a task
   group and an in-memory stream for every request, just to pass the response through. Twice,
   for two middlewares.
2. **Dependencies declared as plain `def`.** FastAPI runs each one in a worker thread, so a feed
   read made 4 thread handoffs and a post 6, for functions that only returned an object.

## The fix

- Both middlewares are plain ASGI classes (`RecordMetrics`, `SecurityHeaders` in
  [`http.py`](../../../feed_fanout/adapters/http.py)): same behavior, same order, no per-request
  task group or stream.
- The four dependencies that only return an object (`get_unit_of_work`, `get_clock`,
  `get_posting_limit`, `get_token_verifier`) are `async def`, so they run on the event loop.
- `current_user` deliberately stays `def`. Verifying a token signed with a key the API hasn't seen
  fetches the identity provider's keys over the network. On the event loop that would freeze
  every request until it returned. Rare, but worth one thread handoff.
- Thread handoffs per request: from 4–6 down to 2 (the token check, and the endpoint, which waits
  on the database: what the thread pool is for).

## Results

50 requests/s for 2 minutes, after a 1-minute warm-up, same machine and same per-service budget
as the baseline (API 1 core / 512 MB, Postgres 2 cores / 1.5 GB). Commit `5f58b57`, run
`20261002-174253` ([report](report.md), [data](data/)).

| At 50 requests/s | Baseline | After | |
|---|---|---|---|
| API CPU (of one core) | 36% | 16% | |
| **CPU per request** | **7.3 ms** | **3.3 ms** | **−55%** |
| Feed read p99 (server) | 764 ms | 13 ms | |
| Post p99 (server) | 973 ms | 24 ms | |
| Post → feed p99 | 26.5 s (the celebrity stall) | 0.97 s | |
| Failed / dropped requests | 0 / 6 | 0 / 0 | |

## Caveats

- **CPU per request is the number to trust.** The budget and the rate are the same, and the CPU
  a request costs doesn't depend on how fast the database answers.
- **The latency gain isn't all this fix.** Since the baseline, the seed builds the feed's indexes
  from sorted data (more compact than inserting into them row by row), and Postgres started
  fresh. Both can make reads faster. The fix removes queuing for threads and the GIL, which
  should cut latency too, but this run can't separate the causes.
- **The new ceiling isn't measured.** Half the CPU per request suggests more headroom than ~95
  requests/s; the next run at higher rates will show how much.
- **No celebrity post happened to be fanned out in this run,** so the post → feed p99 doesn't
  speak to that problem, which this fix doesn't touch.

## What went wrong while measuring

- **The profiler slowed the run it profiled.** py-spy pauses the process for each sample by
  default; at 100 samples a second across the API's 42 threads, the API couldn't keep up with
  50 requests/s (k6 dropped 1,316). The profile's proportions were still usable; the run's
  latency and throughput weren't. The profiler now samples without pausing (`--nonblocking`).
- **The first "after" run was invalid.** The API was rebuilt with only the base compose file, so
  it, and Postgres with it, ran without their resource limits. That run was discarded and
  repeated with the load test's configuration; the limits were checked on the containers.
- The interactive flame graph of the "before" profile was deleted by a careless cleanup; its
  summary, which every number above comes from, was kept.
