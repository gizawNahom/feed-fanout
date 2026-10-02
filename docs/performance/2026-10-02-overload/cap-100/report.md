# Load test: 20261002-184414

```
date: 2026-10-02T18:44:14+03:00
commit: 697b1e5
cpu: 12th Gen Intel(R) Core(TM) i5-1245U, 12 threads
memory (as Docker sees it): 7.6 GiB
kernel: 6.18.40.1-microsoft-standard-WSL2
rates: 100 400; step: 2m; pause: 60
```

**Knee: 100 requests/s**, the last step the system sustained.

## Steps

| Rate (req/s) | Sent (k6) | Served (server) | Failed (k6) | 5xx | of which shed | Dropped | Posts fanned out | Post→feed p50 / p95 / p99 | Lag max (end) | Outbox age max | Sustainable |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | 100/s | 100/s | 0.00% | 0 | 0 | 0 | 935 | 0.30 s / 0.69 s / 0.94 s | 4 (0) | 0.38 s | yes |
| 400 | 397/s | 386/s | 78.28% | 37176 | 37176 | 0 | 908 | 0.46 s / 0.94 s / 0.99 s | 7 (0) | 0.52 s | no |

## Latency per endpoint (p99: clients, from k6 / server, from Prometheus)

| Rate | GET /me/feed | POST /posts | PUT /me/following | DELETE /me/following | Fanout p99 |
|---|---|---|---|---|---|
| 100 | 14 ms / 17 ms | 20 ms / 24 ms | 14 ms / 20 ms | 11 ms / 10 ms | 93 ms |
| 400 | 2029 ms / 2480 ms | 2061 ms / 2482 ms | 1522 ms / 2417 ms | 1579 ms / 2429 ms | 224 ms |

## Containers (average CPU, peak memory / limit)

| Rate | api | worker | relay | postgres | kafka | k6 |
|---|---|---|---|---|---|---|
| 100 | 33%, 97/512 MiB | 2%, 74/256 MiB | 2%, 69/256 MiB | 20%, 974/1536 MiB | 21%, 669/1024 MiB | n/a |
| 400 | 94%, 120/512 MiB | 2%, 71/256 MiB | 2%, 69/256 MiB | 78%, 1212/1536 MiB | 13%, 600/1024 MiB | n/a |

## Why a step wasn't sustainable

- **400/s:** 78.3% of requests failed; GET /me/feed p99 2480 ms (over 2x the first step's 17 ms); POST /posts p99 2482 ms (over 2x the first step's 24 ms); PUT /me/following p99 2417 ms (over 2x the first step's 20 ms); DELETE /me/following p99 2429 ms (over 2x the first step's 10 ms)

- **Sent** is what k6 sent per second; **Served** is what the API answered, as it counted them.
- **Shed** are 503s from overload protection: requests turned away at once because the API was at
  capacity (they count as failed for k6, and as 5xx). Unlike errors, they're the intended behavior.
  The gap is requests that never got an answer the server could count (refused, or cut off).
- Percentiles come from histogram buckets, so they're interpolated within a bucket, not exact;
  "> 60 s" means at or past the top bucket (60 s for latencies, 300 s for post→feed).
- CPU is % of one core (200% = two cores).

