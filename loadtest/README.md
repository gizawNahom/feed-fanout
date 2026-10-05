# Load test

How much traffic the system sustains on one known machine, and what gives out first. It
measures; it doesn't extrapolate. The numbers belong to this resource budget on this machine.

## Run it

```bash
make down              # the load test needs the same ports as the dev stack
make loadtest-setup    # Postgres, seed 100,000 users, then the rest of the stack; tokens
make loadtest          # warm-up + 5 steps of 3 min, with pauses: ~21 min
make loadtest-down     # stop, delete its data (~5 GB)
```

While it runs, watch Grafana at http://localhost:3000: consumer lag, outbox age and the
post-to-feed delay show the knee as it happens. The report is written to
`loadtest/results/<time>/report.md` at the end. **That file is what to share.**

Other rates or durations: `RATES="100 200 300" STEP=5m make loadtest`. If every step is still
sustainable, run higher ones: the point is to find where it stops.

### Before you start

- **Plug in** and choose the best-performance power mode. Close heavy apps.
- **Laptop CPUs slow down when hot.** The first minutes run faster than the rest, so run all
  the steps in one session. Don't compare a cold run with a warm one.
- **Disk:** about 5 GB while it runs. WSL's virtual disk doesn't shrink by itself after
  `make loadtest-down`; compacting it gives the space back to Windows.

## What it does

**Data** (`seed.sql`, straight into Postgres):

- 100,000 users who each follow 10 to 90 others. Popular accounts are picked far more
  often, so follower counts follow a power law: the median user has ~35 followers, the top
  account ~20,000. Those are the posts where fanout-on-write is expensive.
- 5 posts per user over 30 days, already in every follower's feed: ~25 million feed rows,
  about 5 GB. Loaded without the feed's indexes, which are built once at the end: inserting
  into them row by row took ~50 minutes in the first run. That's several times Postgres's memory limit (1.5 GB) on purpose, so feed reads hit
  the disk the way they do at scale, instead of being served entirely from memory.

**Traffic** (`feed.js`, k6), through the API gateway like real clients: requests arrive at a
fixed rate per second, whatever the response times (an open model: slow responses don't quietly lower the load). From 10,000
active users (every 10th user of the graph, so a few celebrities among them):

| Share | Request |
|---|---|
| 90% | Read your feed (`GET /me/feed`, first page) |
| 8% | Post |
| 2% | Follow or unfollow someone (popular accounts more often) |

The posting limit stays on. Spread over 10,000 users, posting stays far below 10 a minute
each, so a 429 would be a finding in itself (counted separately, not as a failure).

**Steps** (`run.sh`): a 1-minute warm-up at 20/s (not reported), then each rate (50, 100, 200,
400, 800/s by default) for 3 minutes, with a 1-minute pause after each for any backlog to
drain. Container CPU and memory are sampled every 5 s throughout.

## Profiling the API

Where the API's CPU time goes, measured rather than guessed. Run a gentle load in one
terminal and the profiler in another:

```bash
RATES=50 STEP=2m make loadtest   # terminal 1: busy, but not overloaded
make profile-api                 # terminal 2, once the load is running: samples for 60 s
```

[py-spy](https://github.com/benfred/py-spy) samples the API's stacks 100 times a second from
a separate, short-lived container that shares its process namespace: nothing is installed in
the API, and it isn't restarted. Only threads holding Python's GIL are sampled, so the profile
is CPU work, not waiting for requests or the database.

It writes two files to `loadtest/results/`:

- `profile-<time>.speedscope.json`: drop it on https://www.speedscope.app for an interactive
  flame graph (the file stays in your browser).
- `profile-<time>.md` ([`profile_summary.py`](profile_summary.py)): the CPU share per library
  and per function. *Self* time is where CPU is burned; *total* time includes everything a
  function calls, so a middleware's total is the cost of everything it wraps.

## The report

`report.py` combines, for each step:

- **k6:** what clients saw. Latency per endpoint, failures, dropped requests.
- **Prometheus:** what the system did. Post-to-feed delay (p50/p95/p99), server latency
  per endpoint, fanout duration, consumer lag, outbox age.
- **Container stats:** CPU and peak memory of each service, against its limit.

A step is **sustainable** if the system could keep it up indefinitely. It isn't if:
- more than 1% of requests failed, or k6 had to drop more than 1%;
- a backlog grew (consumer lag, outbox age);
- an endpoint's p99 doubled from the first step;
- a service other than Postgres neared its memory limit.

**The knee is the last sustainable step.** No SLO is applied: the post-to-feed delay is
measured, and the SLO is chosen from it afterwards.

## The budget

Limits are caps, not reservations: a service uses what it needs, up to its limit
(`docker-compose.loadtest.yml`).

| Service | CPU | Memory |
|---|---|---|
| API gateway (Kong) | 1 | 512 MB |
| API (2 instances behind the gateway) | 1 each | 256 MB each |
| Workers (3, one per partition) | 0.5 each | 256 MB each |
| Relay | 0.5 | 256 MB |
| Postgres | 2 | 1.5 GB |
| Kafka (1 broker, 512 MB heap) | 1 | 1 GB |
| k6 | 1 | 1 GB |
| Prometheus, Grafana, Loki, Alloy, exporter | 2.5 in total | 1.9 GB in total |

About 7.4 GB in the worst case, on a WSL with 7.6 GB; the expected peak is 4 to 5 GB. Kafka UI and
Adminer aren't started.

## Limits of this test

- **One machine, one of each service.** It finds this setup's knee. It says nothing about
  what more instances or bigger machines would do. That would be a different test.
- **Everything shares one CPU,** including k6. The caps keep services from taking each other's
  share, but not from competing for the same cores and memory bandwidth.
- **First feed page only.** Scrolling deeper uses the same index, but isn't exercised.
