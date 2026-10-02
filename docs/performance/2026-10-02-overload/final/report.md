# Load test: 20261002-202013

```
date: 2026-10-02T20:20:13+03:00
commit: 061dd9f
cpu: 12th Gen Intel(R) Core(TM) i5-1245U, 12 threads
memory (as Docker sees it): 7.6 GiB
kernel: 6.18.40.1-microsoft-standard-WSL2
rates: 100 400; step: 2m; pause: 60
```

**Knee: 100 requests/s**, the last step the system sustained.

## Steps

| Rate (req/s) | Sent (k6) | Served (server) | Failed (k6) | 5xx | of which shed | Dropped | Posts fanned out | Post→feed p50 / p95 / p99 | Lag max (end) | Outbox age max | Sustainable |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | 100/s | 100/s | 0.00% | 0 | 0 | 0 | 1023 | 0.31 s / 0.77 s / 0.95 s | 5 (0) | 0.49 s | yes |
| 400 | 400/s | 397/s | 63.23% | 30571 | 30571 | 0 | 1448 | 0.43 s / 0.93 s / 0.99 s | 10 (0) | 0.50 s | no |

## Latency per endpoint (p99: clients, from k6 / server, from Prometheus)

| Rate | GET /me/feed | POST /posts | PUT /me/following | DELETE /me/following | Fanout p99 |
|---|---|---|---|---|---|
| 100 | 17 ms / 22 ms | 30 ms / 35 ms | 20 ms / 24 ms | 20 ms / 25 ms | 135 ms |
| 400 | 381 ms / 481 ms | 441 ms / 494 ms | 279 ms / 422 ms | 522 ms / 443 ms | 168 ms |

## Containers (average CPU, peak memory / limit)

| Rate | api | worker | relay | postgres | kafka | k6 |
|---|---|---|---|---|---|---|
| 100 | 35%, 97/512 MiB | 2%, 73/256 MiB | 2%, 64/256 MiB | 22%, 1271/1536 MiB | 19%, 663/1024 MiB | n/a |
| 400 | 94%, 97/512 MiB | 3%, 74/256 MiB | 2%, 64/256 MiB | 40%, 1286/1536 MiB | 23%, 690/1024 MiB | n/a |

## Why a step wasn't sustainable

- **400/s:** 63.2% of requests failed; GET /me/feed p99 481 ms (over 2x the first step's 22 ms); POST /posts p99 494 ms (over 2x the first step's 35 ms); PUT /me/following p99 422 ms (over 2x the first step's 24 ms); DELETE /me/following p99 443 ms (over 2x the first step's 25 ms)

- **Sent** is what k6 sent per second; **Served** is what the API answered, as it counted them.
- **Shed** are 503s from overload protection: requests turned away at once because the API was at
  capacity (they count as failed for k6, and as 5xx). Unlike errors, they're the intended behavior.
  The gap is requests that never got an answer the server could count (refused, or cut off).
- Percentiles come from histogram buckets, so they're interpolated within a bucket, not exact;
  "> 60 s" means at or past the top bucket (60 s for latencies, 300 s for post→feed).
- CPU is % of one core (200% = two cores).

