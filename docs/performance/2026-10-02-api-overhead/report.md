# Load test: 20261002-174253

```
date: 2026-10-02T17:42:53+03:00
commit: 5f58b57
cpu: 12th Gen Intel(R) Core(TM) i5-1245U, 12 threads
memory (as Docker sees it): 7.6 GiB
kernel: 6.18.40.1-microsoft-standard-WSL2
rates: 50; step: 2m; pause: 60
```

**Knee: 50 requests/s**, the last step the system sustained.

## Steps

| Rate (req/s) | Sent (k6) | Served (server) | Failed (k6) | 5xx | Dropped | Posts fanned out | Post→feed p50 / p95 / p99 | Lag max (end) | Outbox age max | Sustainable |
|---|---|---|---|---|---|---|---|---|---|---|
| 50 | 50/s | 50/s | 0.00% | 0 | 0 | 477 | 0.33 s / 0.83 s / 0.97 s | 1 (0) | 0.42 s | yes |

## Latency per endpoint (p99: clients, from k6 / server, from Prometheus)

| Rate | GET /me/feed | POST /posts | PUT /me/following | DELETE /me/following | Fanout p99 |
|---|---|---|---|---|---|
| 50 | 11 ms / 13 ms | 18 ms / 24 ms | 22 ms / 23 ms | 17 ms / 34 ms | 94 ms |

## Containers (average CPU, peak memory / limit)

| Rate | api | worker | relay | postgres | kafka | k6 |
|---|---|---|---|---|---|---|
| 50 | 16%, 93/512 MiB | 1%, 68/256 MiB | 2%, 76/256 MiB | 10%, 581/1536 MiB | 22%, 647/1024 MiB | n/a |

## Why a step wasn't sustainable

- Every step was.

- **Sent** is what k6 sent per second; **Served** is what the API answered, as it counted them.
  The gap is requests that never got an answer the server could count (refused, or cut off).
- Percentiles come from histogram buckets, so they're interpolated within a bucket, not exact;
  "> 60 s" means at or past the top bucket (60 s for latencies, 300 s for post→feed).
- CPU is % of one core (200% = two cores).

