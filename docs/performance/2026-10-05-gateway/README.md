# API gateway and scaling out, 2026-10-05

Kong now sits in front of the API ([README](../../../README.md#api-gateway)): it routes the
public API, balances across API instances, and refuses excess load at the edge. Two questions:
does the gateway do its job under load, and do more API instances serve more?

## Summary

- **The gateway does its job.** At 400 requests/s offered, it held the API to its configured
  limit (250/s) and refused the excess **at the edge**: 150/s, with `429` and a retry hint,
  before they reached Python. It added about 1 ms (time spent in Kong: p99 1.0 ms at 100/s,
  1.3 ms at 400/s, measured in the first run).
- **Two layers of overload protection, no errors.** What still exceeded an API instance's own
  capacity, the API turned away itself (`503`). Not one 5xx error in any run.
- **Scaling out works, but not 2×, on this laptop:** 1 instance served 116 requests/s, 2
  instances 170/s (**+47%**), back to back under identical conditions. The likely limit is the
  machine: 2 fast cores, shared by the API processes, Kong, Postgres, Kafka and the load
  generator. Showing linear scaling needs a machine with more fast cores.

## Setup

Same machine, data and budget as the [baseline](../2026-09-30-baseline/README.md), commit
`29103cc`. Traffic goes through Kong (1 core, 512 MB); each API instance gets 1 core and 256 MB;
3 fanout workers. Each run: 100 then 400 requests/s, 2 minutes each, after a 1-minute warm-up.

For the comparison runs, heavy desktop apps were closed and the log pipeline (Alloy, Loki) and
Grafana were paused: at 400 requests/s, Kong's access log alone is 400 lines a second to ship.
Then the two runs, back to back, changing only the number of API instances.

## Results

At 400 requests/s offered:

| | A: 1 API instance | B: 2 API instances |
|---|---|---|
| **Really served** | **116/s** | **170/s** |
| Refused at the gateway (429) | 150/s | 150/s |
| Turned away by the API (503) | 135/s | 80/s |
| Errors | 0 | 0 |
| p99 of admitted requests (server) | 473 ms | 495 ms |
| CPU: API instances, together | 73% | 161% (~80% each) |
| CPU: Kong / Postgres | 27% / 27% | 44% / 59% |
| Post → feed p99 | 0.99 s | 0.99 s |

At 100/s, both served everything, nothing turned away, p99 21 ms.

*Really served* is the report's *Served* minus *shed*; CPU is a share of one core.
Reports: [A](a-1-instance/report.md), [B](b-2-instances/report.md); raw data next to them.

## What it shows

**The gateway refuses cheaply and adds little.** Kong passed 250 requests/s and refused the
other 150 with 429, as configured. Its own time per request stayed around 1 ms at p99 (first
run), using under half a core for 400 requests/s. Refusing a request in Python cost ~1.8 ms of CPU
([overload entry](../2026-10-02-overload/README.md)); at the edge, the API never sees it.

**Scaling out: +47%, and why not 2×.** The API instances weren't starved for work: 2 instances
turned away fewer requests than 1 (80/s vs 135/s), and served more. But the machine has 2
performance cores (and 8 efficiency cores), and in run B, two API processes, Kong, Postgres
(59%), Kafka and the load generator were all busy at once. The same API code spent ~6 ms of CPU
per served request in A and ~9.5 ms in B: slower core-seconds, not more work. A first run today,
with desktop apps open and the log pipeline running, served 148/s with 2 instances
([report](first-run/report.md)); closing them took it to 170/s. The machine moves this number.

**An API instance's limit under overload isn't raw CPU.** With one instance, it turned requests
away at ~73% CPU. Its cap of 30 requests in flight is reached first: requests take turns on
Python's single interpreter lock (the GIL), each one waiting ~0.25–0.5 s under load, so 30 in
flight serve ~100 a second (Little's law: in flight = throughput × time). More processes, each
with its own interpreter, is the way past it, which is what scaling out does.

## What's left

- **Linear scaling isn't shown.** Run the same A/B on a machine with more fast cores (a cloud VM)
  to separate the system's limits from the laptop's.
- **The gateway's limit is global and fixed** (250/s): set by hand to match the instances, and
  shared by all clients. Per-client limits need clients identified at the edge.
- **One gateway instance;** in production, at least two, behind the cloud's load balancer.
