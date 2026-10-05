# Load test: 20261005-115638

```
date: 2026-10-05T11:56:38+03:00
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
| 100 | 100/s | 101/s | 0.00% | 0 | 0 | 0 | 0 | 974 | 0.35 s / 0.87 s / 0.98 s | 5 (5) | 0.45 s | yes |
| 400 | 400/s | 250/s | 25.46% | 17661 | 12221 | 12221 | 0 | 1437 | 0.59 s / 0.98 s / 1.61 s | 7 (0) | 0.55 s | no |

## Latency per endpoint (p99: clients, from k6 / server, from Prometheus)

| Rate | GET /me/feed | POST /posts | PUT /me/following | DELETE /me/following | Fanout p99 |
|---|---|---|---|---|---|
| 100 | 24 ms / 24 ms | 30 ms / 42 ms | 35 ms / 36 ms | 20 ms / 23 ms | 223 ms |
| 400 | 515 ms / 595 ms | 621 ms / 927 ms | 449 ms / 497 ms | 469 ms / 492 ms | 371 ms |

## Containers (average CPU, peak memory / limit)

| Rate | api | worker | relay | postgres | kafka | k6 |
|---|---|---|---|---|---|---|
| 100 | 21%, 89/256 MiB | 2%, 65/256 MiB | 2%, 64/256 MiB | 31%, 1231/1536 MiB | 5%, 489/1024 MiB | n/a |
| 400 | 81%, 106/256 MiB | 4%, 69/256 MiB | 3%, 73/256 MiB | 69%, 1109/1536 MiB | 45%, 597/1024 MiB | n/a |

## Why a step wasn't sustainable

- **400/s:** 25.5% of requests failed; GET /me/feed p99 595 ms (over 2x the first step's 24 ms); POST /posts p99 927 ms (over 2x the first step's 42 ms); PUT /me/following p99 497 ms (over 2x the first step's 36 ms); DELETE /me/following p99 492 ms (over 2x the first step's 23 ms)

- **Sent** is what k6 sent per second; **Served** is what the API answered, as it counted them.
- **Refused at gateway** are 429s from Kong's global rate limit: turned away at the edge, before
  reaching the API. They count as failed for k6.
- **Shed** are 503s from overload protection: requests turned away at once because the API was at
  capacity (they count as failed for k6, and as 5xx). Unlike errors, they're the intended behavior.
  The gap is requests that never got an answer the server could count (refused, or cut off).
- Percentiles come from histogram buckets, so they're interpolated within a bucket, not exact;
  "> 60 s" means at or past the top bucket (60 s for latencies, 300 s for post→feed).
- CPU is % of one core (200% = two cores).

