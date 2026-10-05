# Load test: 20261005-121545

```
date: 2026-10-05T12:15:45+03:00
commit: 29103cc
cpu: 12th Gen Intel(R) Core(TM) i5-1245U, 12 threads
memory (as Docker sees it): 7.6 GiB
kernel: 6.18.40.1-microsoft-standard-WSL2
rates: 100 400; step: 2m; pause: 60
```

**Knee: 100 requests/s**, the last step the system sustained.

## Steps

| Rate (req/s) | Sent (k6) | Served (server) | Failed (k6) | Refused at gateway | 5xx | of which shed | Dropped | Posts fanned out | Post→feed p50 / p95 / p99 | Lag max (end) | Outbox age max | Sustainable |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | 100/s | 100/s | 0.00% | 0 | 0 | 0 | 0 | 933 | 0.31 s / 0.75 s / 0.95 s | 5 (0) | 0.43 s | yes |
| 400 | 400/s | 251/s | 33.83% | 18300 | 16497 | 16497 | 0 | 1158 | 0.42 s / 0.93 s / 0.99 s | 5 (0) | 0.42 s | no |

## Latency per endpoint (p99: clients, from k6 / server, from Prometheus)

| Rate | GET /me/feed | POST /posts | PUT /me/following | DELETE /me/following | Fanout p99 |
|---|---|---|---|---|---|
| 100 | 14 ms / 17 ms | 23 ms / 25 ms | 16 ms / 23 ms | 13 ms / 22 ms | 165 ms |
| 400 | 304 ms / 467 ms | 372 ms / 491 ms | 272 ms / 374 ms | 280 ms / 438 ms | 212 ms |

## Containers (average CPU, peak memory / limit)

| Rate | api | worker | relay | postgres | kafka | k6 |
|---|---|---|---|---|---|---|
| 100 | 29%, 101/256 MiB | 2%, 69/256 MiB | 2%, 64/256 MiB | 21%, 1063/1536 MiB | 19%, 620/1024 MiB | n/a |
| 400 | 73%, 104/256 MiB | 3%, 69/256 MiB | 2%, 64/256 MiB | 27%, 1279/1536 MiB | 32%, 581/1024 MiB | n/a |

## Why a step wasn't sustainable

- **400/s:** 33.8% of requests failed; GET /me/feed p99 467 ms (over 2x the first step's 17 ms); POST /posts p99 491 ms (over 2x the first step's 25 ms); PUT /me/following p99 374 ms (over 2x the first step's 23 ms); DELETE /me/following p99 438 ms (over 2x the first step's 22 ms)

- **Sent** is what k6 sent per second; **Served** is what the API answered, as it counted them.
- **Refused at gateway** are 429s from Kong's global rate limit: turned away at the edge, before
  reaching the API. They count as failed for k6.
- **Shed** are 503s from overload protection: requests turned away at once because the API was at
  capacity (they count as failed for k6, and as 5xx). Unlike errors, they're the intended behavior.
  The gap is requests that never got an answer the server could count (refused, or cut off).
- Percentiles come from histogram buckets, so they're interpolated within a bucket, not exact;
  "> 60 s" means at or past the top bucket (60 s for latencies, 300 s for post→feed).
- CPU is % of one core (200% = two cores).

