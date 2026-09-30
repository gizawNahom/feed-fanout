# Load test: 20260930-213242

```
date: 2026-09-30T21:32:42+03:00
commit: 8d3180d
cpu: 12th Gen Intel(R) Core(TM) i5-1245U, 12 threads
memory (as Docker sees it): 7.6 GiB
kernel: 6.18.40.1-microsoft-standard-WSL2
rates: 50 100 200 400 800; step: 3m; pause: 60
```

**Knee: 50 requests/s**, the last step the system sustained.

## Steps

| Rate (req/s) | Sent (k6) | Served (server) | Failed (k6) | 5xx | Dropped | Posts fanned out | Post→feed p50 / p95 / p99 | Lag max (end) | Outbox age max | Sustainable |
|---|---|---|---|---|---|---|---|---|---|---|
| 50 | 50/s | 50/s | 0.00% | 0 | 6 | 726 | 0.43 s / 12.59 s / 26.52 s | 75 (3) | 0.52 s | yes |
| 100 | 100/s | 100/s | 0.00% | 0 | 60 | 1480 | 0.46 s / 0.95 s / 0.99 s | 8 (4) | 0.56 s | no |
| 200 | 156/s | 93/s | 54.06% | 4190 | 6929 | 1052 | 0.57 s / 0.97 s / 1.26 s | 6 (28) | 0.40 s | no |
| 400 | 370/s | 90/s | 81.95% | 4140 | 3011 | 1005 | 0.57 s / 0.97 s / 1.52 s | 7 (1) | 0.59 s | no |
| 800 | 555/s | 70/s | 89.45% | 2284 | 38966 | 845 | 0.69 s / 1.53 s / 1.91 s | 15 (5) | 0.46 s | no |

## Latency per endpoint (p99: clients, from k6 / server, from Prometheus)

| Rate | GET /me/feed | POST /posts | PUT /me/following | DELETE /me/following | Fanout p99 |
|---|---|---|---|---|---|
| 50 | 791 ms / 764 ms | 1124 ms / 973 ms | 497 ms / 490 ms | 588 ms / 768 ms | 322 ms |
| 100 | 1534 ms / 2117 ms | 2256 ms / 2227 ms | 1330 ms / 2020 ms | 1224 ms / 1859 ms | 226 ms |
| 200 | 14171 ms / > 10 s | 18935 ms / > 10 s | 11748 ms / > 10 s | 11062 ms / > 10 s | 198 ms |
| 400 | 14003 ms / > 10 s | 19429 ms / > 10 s | 11845 ms / > 10 s | 11094 ms / > 10 s | 204 ms |
| 800 | 16452 ms / > 10 s | 21434 ms / > 10 s | 12953 ms / > 10 s | 12792 ms / > 10 s | 207 ms |

## Containers (average CPU, peak memory / limit)

| Rate | api | worker | relay | postgres | kafka | k6 |
|---|---|---|---|---|---|---|
| 50 | 36%, 100/512 MiB | 3%, 70/256 MiB | 3%, 65/256 MiB | 26%, 1198/1536 MiB | 47%, 624/1024 MiB | n/a |
| 100 | 73%, 118/512 MiB | 4%, 67/256 MiB | 2%, 65/256 MiB | 42%, 1278/1536 MiB | 42%, 638/1024 MiB | n/a |
| 200 | 94%, 200/512 MiB | 2%, 67/256 MiB | 2%, 65/256 MiB | 40%, 1279/1536 MiB | 42%, 618/1024 MiB | n/a |
| 400 | 98%, 200/512 MiB | 3%, 68/256 MiB | 2%, 73/256 MiB | 43%, 1206/1536 MiB | 18%, 626/1024 MiB | n/a |
| 800 | 94%, 204/512 MiB | 3%, 68/256 MiB | 2%, 69/256 MiB | 42%, 1229/1536 MiB | 40%, 649/1024 MiB | n/a |

## Why a step wasn't sustainable

- **100/s:** GET /me/feed p99 2117 ms (over 2x the first step's 764 ms); POST /posts p99 2227 ms (over 2x the first step's 973 ms); PUT /me/following p99 2020 ms (over 2x the first step's 490 ms); DELETE /me/following p99 1859 ms (over 2x the first step's 768 ms)
- **200/s:** k6 dropped 6929 requests (responses too slow to keep the rate); 54.1% of requests failed; GET /me/feed p99 > 10 s (over 2x the first step's 764 ms); POST /posts p99 > 10 s (over 2x the first step's 973 ms); PUT /me/following p99 > 10 s (over 2x the first step's 490 ms); DELETE /me/following p99 > 10 s (over 2x the first step's 768 ms)
- **400/s:** k6 dropped 3011 requests (responses too slow to keep the rate); 81.9% of requests failed; GET /me/feed p99 > 10 s (over 2x the first step's 764 ms); POST /posts p99 > 10 s (over 2x the first step's 973 ms); PUT /me/following p99 > 10 s (over 2x the first step's 490 ms); DELETE /me/following p99 > 10 s (over 2x the first step's 768 ms)
- **800/s:** k6 dropped 38966 requests (responses too slow to keep the rate); 89.5% of requests failed; GET /me/feed p99 > 10 s (over 2x the first step's 764 ms); POST /posts p99 > 10 s (over 2x the first step's 973 ms); PUT /me/following p99 > 10 s (over 2x the first step's 490 ms); DELETE /me/following p99 > 10 s (over 2x the first step's 768 ms)

- **Sent** is what k6 sent per second; **Served** is what the API answered, as it counted them.
  The gap is requests that never got an answer the server could count (refused, or cut off).
- Percentiles come from histogram buckets, so they're interpolated within a bucket, not exact;
  "> 10 s" means at or past the top bucket (10 s for latencies, 300 s for post→feed).
- CPU is % of one core (200% = two cores).

