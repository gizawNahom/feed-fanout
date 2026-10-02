# Overload: from collapse to graceful, 2026-10-02

The [baseline](../2026-09-30-baseline/README.md) showed the API collapsing past its capacity:
requests piled up until it ran out of file descriptors, and it served *less* the harder it was
pushed. This entry is the fix, measured in three runs, plus two bugs those runs exposed.

## Summary

At 400 requests/s offered, far more than one API process can serve:

| | Baseline | Cap 100 | Cap 30 | Final |
|---|---|---|---|---|
| Really served | ~90/s | 86/s | 101/s | **146/s** |
| Turned away (immediate 503) | none: they piled up | 300/s | 289/s | 251/s |
| Errors (500s) | 4,140 | 0 | 0 | **0** |
| Requests that got no answer | thousands | 0 | 0 | **0** |
| p99 of admitted requests | over 10 s (14 s measured) | 2.5 s | 0.8–0.9 s | **under 0.5 s** |
| Postgres CPU (of one core) | 43% | 78% | 77% | **40%** |
| Post → feed p99 | 1.5 s | 0.99 s | 0.99 s | **0.99 s** |

Overloaded, the API now serves at capacity, turns the excess away at once with a retry hint,
answers what it admits in under half a second, and produces no errors. At 100/s, every run
served everything with nothing turned away, at a p99 of 17–35 ms.

*Really served* is the report's *Served* minus *shed*, both counted by the API itself; the
baseline didn't shed. Same machine and budget as the baseline, with 3 fanout workers.

## The changes, one run each

**Run 1, cap 100** (commit `697b1e5`, [report](cap-100/report.md)):
- **Overload protection** (`ShedLoad` in [`http.py`](../../../feed_fanout/adapters/http.py)): past a
  cap on requests in flight, new ones get an immediate `503` with `Retry-After: 1`, instead of
  queuing. Health checks and `/metrics` are never turned away.
- **The API's file limit** raised from 1,024 to 65,536.
- **3 fanout workers**, one per partition, in the load test (the dev stack keeps 1).
- **Result:** no errors, no lost connections, no collapse. But only 86/s really served, and the
  requests it admitted waited up to 2.5 s.

**Run 2, cap 30** (`507b833`, [report](cap-30/report.md)):
- **Why the wait:** a cap of 100 admitted far more than one API process works on at once (40
  threads, ~15 database connections). The rest queued *inside* the API: in flight = rate × wait
  (Little's law), so 100 in flight at ~86/s is over a second of waiting. At 30, admitted p99 went
  from 2.5 s to 0.8–0.9 s, and served rose to 101/s.
- **Also meant to turn off the per-request access log** (`--no-access-log`), since turning a
  request away cost ~2.2 ms of CPU, nearly as much as serving one. It didn't take effect (bug 1).

**Run 3, final** (`061dd9f`, [report](final/report.md)):
- **The access log really off** (bug 1, fixed).
- **Database connections kept and reused** (bug 2, fixed).
- **Result:** 146/s served under overload, admitted p99 under 0.5 s, Postgres back to 40% CPU.

## Two bugs the runs exposed

**1. `--no-access-log` was silently undone.** uvicorn turns its access logger off by removing
its handlers and stopping it passing lines up. Our logging setup, run when the app is imported,
switched the passing-up back on. Run 2 logged 55,098 feed requests it was meant not to. Fixed in
[`logs.py`](../../../feed_fanout/logs.py); a test covers both states and fails without the fix.

**2. Database connections churned.** SQLAlchemy's default pool keeps 5 connections and opens up
to 10 more under load, closing those as soon as they're returned. The API needed more than 5 at
once, so it opened and closed connections constantly: Postgres had started 13,302 sessions in
about three hours of tests, a process and an authentication each, and used 77% CPU to serve
~100 requests/s. Now the pool keeps up to 20 and reuses them (`DB_POOL_SIZE`, no overflow). In
run 3, the session count rose by 332, which is the background rate: ~25 a minute, measured idle,
from the metrics exporter and Postgres's health check. The API itself opened essentially none.

## Found on the way: the SLO alert could never fire

The SLO rules selected the bucket `le="5"`, but the client library writes bounds as floats, so
the stored label is `le="5.0"`, and Prometheus matches label values exactly. The recording rules
were always empty: the burn alert could never fire, and the dashboard's SLO panel showed no
data. The promtool tests passed, because their synthetic series used the same wrong label.
Confirmed against live data, fixed, and the metric contract test now checks every bucket
selector in the rules against the buckets the app exposes.

The SLO itself is now **99% of posts in feeds within 2 s**, set from these measurements: clean
runs had no post over 1 s. The risk to it is a celebrity's fanout: one run that had one saw
11.7% of its posts take over 2 s.

## What's left

- **Turning a request away still costs ~1.8 ms of CPU** in the API (parsing, middleware, the 503).
  In production, excess load is best refused before it reaches Python: at the load balancer
  or reverse proxy, which do it for a fraction of the cost.
- **More capacity is horizontal:** one API process uses one core. Not measured here.
- **The capacity without overload isn't measured either:** a step between 100 and 400/s would
  find where admitted latency starts to rise.
