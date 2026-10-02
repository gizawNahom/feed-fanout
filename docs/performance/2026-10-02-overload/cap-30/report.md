# Load test: 20261002-194100

```
date: 2026-10-02T19:41:00+03:00
commit: 507b833
cpu: 12th Gen Intel(R) Core(TM) i5-1245U, 12 threads
memory (as Docker sees it): 7.6 GiB
kernel: 6.18.40.1-microsoft-standard-WSL2
rates: 100 400; step: 2m; pause: 60
```

**Knee: 100 requests/s**, the last step the system sustained.

## Steps

| Rate (req/s) | Sent (k6) | Served (server) | Failed (k6) | 5xx | of which shed | Dropped | Posts fanned out | Post→feed p50 / p95 / p99 | Lag max (end) | Outbox age max | Sustainable |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | 100/s | 100/s | 0.00% | 0 | 0 | 0 | 965 | 0.30 s / 0.74 s / 0.95 s | 4 (2) | 0.48 s | yes |
| 400 | 399/s | 391/s | 74.03% | 35313 | 35313 | 0 | 1028 | 0.43 s / 0.93 s / 0.99 s | 1 (0) | 0.44 s | no |

## Latency per endpoint (p99: clients, from k6 / server, from Prometheus)

| Rate | GET /me/feed | POST /posts | PUT /me/following | DELETE /me/following | Fanout p99 |
|---|---|---|---|---|---|
| 100 | 16 ms / 21 ms | 25 ms / 25 ms | 19 ms / 24 ms | 17 ms / 23 ms | 97 ms |
| 400 | 578 ms / 773 ms | 654 ms / 914 ms | 590 ms / 757 ms | 531 ms / 499 ms | 177 ms |

## Containers (average CPU, peak memory / limit)

| Rate | api | worker | relay | postgres | kafka | k6 |
|---|---|---|---|---|---|---|
| 100 | 34%, 90/512 MiB | 2%, 67/256 MiB | 1%, 69/256 MiB | 22%, 1267/1536 MiB | 20%, 669/1024 MiB | n/a |
| 400 | 93%, 103/512 MiB | 2%, 73/256 MiB | 2%, 76/256 MiB | 77%, 1272/1536 MiB | 24%, 681/1024 MiB | n/a |

## Why a step wasn't sustainable

- **400/s:** 74.0% of requests failed; GET /me/feed p99 773 ms (over 2x the first step's 21 ms); POST /posts p99 914 ms (over 2x the first step's 25 ms); PUT /me/following p99 757 ms (over 2x the first step's 24 ms); DELETE /me/following p99 499 ms (over 2x the first step's 23 ms)

- **Sent** is what k6 sent per second; **Served** is what the API answered, as it counted them.
- **Shed** are 503s from overload protection: requests turned away at once because the API was at
  capacity (they count as failed for k6, and as 5xx). Unlike errors, they're the intended behavior.
  The gap is requests that never got an answer the server could count (refused, or cut off).
- Percentiles come from histogram buckets, so they're interpolated within a bucket, not exact;
  "> 60 s" means at or past the top bucket (60 s for latencies, 300 s for post→feed).
- CPU is % of one core (200% = two cores).

