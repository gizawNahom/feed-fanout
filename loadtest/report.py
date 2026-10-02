"""
Turn a load test run (loadtest/results/<time>/) into a Markdown report: one row per step, from
three sources, and where the knee is.

    python3 loadtest/report.py loadtest/results/<time>     (loadtest/run.sh runs it at the end)

- k6 (k6-<rate>.json): what clients saw. Latency per endpoint, failures, and dropped
  iterations (k6 couldn't keep the rate because responses were too slow).
- Prometheus (queried for each step's time window, so run this while the stack is still up):
  what the system did. Post-to-feed delay, server latency, consumer lag, outbox age.
- Container stats (stats.csv): CPU and memory of each service.

A step is **sustainable** when the system keeps up with it indefinitely. It isn't if:
- k6 dropped more than 1% of the requests it should have sent, or more than 1% failed;
- a backlog grew: consumer lag ended the step above 1,000 and higher than it started, or the
  oldest outbox event waited more than 30 s;
- server p99 latency for an endpoint more than doubled from the first step;
- a service other than Postgres used over 90% of its memory limit (Postgres fills its memory
  with cached data by design).
The knee is the last sustainable step. No SLO is applied: the numbers are measured, and the
SLO is chosen from them afterwards.

Standard library only: runs with any python3.
"""

import csv
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

PROMETHEUS = os.environ.get("PROMETHEUS", "http://localhost:9090")
ENDPOINTS = ["GET /me/feed", "POST /posts", "PUT /me/following", "DELETE /me/following"]
SERVICES = ["api", "worker", "relay", "postgres", "kafka", "k6"]


@dataclass
class Step:
    rate: int
    start: int
    end: int
    k6: dict[str, dict[str, float]] = field(default_factory=dict)
    server: dict[str, float | None] = field(default_factory=dict)
    containers: dict[str, tuple[float, float, float]] = field(default_factory=dict)  # cpu%, mem MiB, limit MiB
    problems: list[str] = field(default_factory=list)

    @property
    def seconds(self) -> int:
        return self.end - self.start


# --- Reading ------------------------------------------------------------------------------


def read_steps(run: Path) -> list[Step]:
    with (run / "steps.csv").open() as f:
        steps = [Step(int(r["rate"]), int(r["start"]), int(r["end"])) for r in csv.DictReader(f)]
    for step in steps:
        summary = run / f"k6-{step.rate}.json"
        if summary.exists():
            step.k6 = json.loads(summary.read_text())["metrics"]
    return steps


def prometheus(query: str, at: int) -> float | None:
    """One number from Prometheus at a moment, or None (no data, or Prometheus unreachable)."""
    url = f"{PROMETHEUS}/api/v1/query?" + urllib.parse.urlencode({"query": query, "time": at})
    try:
        with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310 (a fixed http URL)
            result = json.load(response)["data"]["result"]
    except (urllib.error.URLError, OSError, KeyError, ValueError):
        return None
    if not result:
        return None
    value = float(result[0]["value"][1])
    return None if value != value else value  # NaN: nothing happened in the window


def read_server(step: Step) -> None:
    w = f"{step.seconds}s"
    q = step.server
    for name, quantile in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99)):
        q[f"post_to_feed_{name}"] = prometheus(
            f"histogram_quantile({quantile}, sum by (le) (increase(feed_fanout_post_to_feed_seconds_bucket[{w}])))",
            step.end,
        )
    q["posts_fanned_out"] = prometheus(f"sum(increase(feed_fanout_post_to_feed_seconds_count[{w}]))", step.end)
    q["fanout_p99"] = prometheus(
        f"histogram_quantile(0.99, sum by (le) (increase(feed_fanout_worker_fanout_duration_seconds_bucket[{w}])))",
        step.end,
    )
    for endpoint in ENDPOINTS:
        method, path = endpoint.split(" ", 1)
        route = "/me/following/{followee_id}" if "following" in path else path
        q[f"p99 {endpoint}"] = prometheus(
            "histogram_quantile(0.99, sum by (le) (increase(feed_fanout_http_request_duration_seconds_bucket"
            f'{{method="{method}",route="{route}"}}[{w}])))',
            step.end,
        )
    served = prometheus(f"sum(increase(feed_fanout_http_requests_total[{w}]))", step.end)
    q["served_per_second"] = None if served is None else served / step.seconds
    # "or vector(0)": no 5xx at all means no such series, which should read as 0, not "no data"
    q["server_5xx"] = prometheus(
        f'sum(increase(feed_fanout_http_requests_total{{status=~"5.."}}[{w}])) or vector(0)', step.end
    )
    q["server_5xx_ratio"] = prometheus(
        f'sum(increase(feed_fanout_http_requests_total{{status=~"5.."}}[{w}]))'
        f" / sum(increase(feed_fanout_http_requests_total[{w}]))",
        step.end,
    )
    q["lag_start"] = prometheus("sum(feed_fanout_consumer_group_lag)", step.start)
    q["lag_end"] = prometheus("sum(feed_fanout_consumer_group_lag)", step.end)
    q["lag_max"] = prometheus(f"max_over_time(sum(feed_fanout_consumer_group_lag)[{w}:5s])", step.end)
    q["outbox_age_max"] = prometheus(f"max_over_time(max(feed_fanout_outbox_oldest_age_seconds)[{w}:5s])", step.end)


_SIZE = re.compile(r"([\d.]+)\s*([KMG]i?B|B)")
_MIB = {"B": 1 / 2**20, "KiB": 1 / 1024, "KB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}


def mib(text: str) -> float:
    m = _SIZE.search(text)
    return float(m.group(1)) * _MIB[m.group(2)] if m else 0.0


def read_containers(run: Path, steps: list[Step]) -> None:
    """Per step and service: average CPU, peak memory, and the memory limit."""
    samples: list[tuple[int, str, float, float, float]] = []
    stats = run / "stats.csv"
    if not stats.exists():
        return
    for row in csv.reader(stats.open()):
        if len(row) < 4:
            continue
        at, name, cpu, mem = row[0], row[1], row[2], row[3]
        service = re.sub(r"^feed-fanout-load-|-\d+$", "", name)
        used, _, limit = mem.partition("/")
        samples.append((int(at), service, float(cpu.rstrip("%") or 0), mib(used), mib(limit)))
    for step in steps:
        for service in SERVICES:
            window = [s for s in samples if s[1] == service and step.start <= s[0] <= step.end]
            if window:
                step.containers[service] = (
                    sum(s[2] for s in window) / len(window),
                    max(s[3] for s in window),
                    window[-1][4],
                )


# --- Judging ------------------------------------------------------------------------------


def judge(steps: list[Step]) -> None:
    baseline = steps[0].server if steps else {}
    for step in steps:
        expected = step.rate * step.seconds
        dropped = step.k6.get("dropped_iterations", {}).get("count", 0)
        if expected and dropped > 0.01 * expected:
            step.problems.append(f"k6 dropped {dropped:.0f} requests (responses too slow to keep the rate)")
        failed = step.k6.get("http_req_failed", {}).get("value", 0)
        if failed > 0.01:
            step.problems.append(f"{failed:.1%} of requests failed")
        lag_start, lag_end = step.server.get("lag_start") or 0, step.server.get("lag_end") or 0
        if lag_end > 1000 and lag_end > lag_start:
            step.problems.append(f"consumer lag grew to {lag_end:.0f}")
        outbox = step.server.get("outbox_age_max") or 0
        if outbox > 30:
            step.problems.append(f"outbox events waited up to {outbox:.0f}s")
        for endpoint in ENDPOINTS:
            now, before = step.server.get(f"p99 {endpoint}"), baseline.get(f"p99 {endpoint}")
            if step is not steps[0] and now and before and now > 2 * before:
                step.problems.append(f"{endpoint} p99 {ms(now)} (over 2x the first step's {ms(before)})")
        for service, (_, peak, limit) in step.containers.items():
            # Postgres fills its memory with cached data on purpose: near its limit is normal.
            if service != "postgres" and limit and peak > 0.9 * limit:
                step.problems.append(f"{service} memory at {peak:.0f}/{limit:.0f} MiB")


# --- Writing ------------------------------------------------------------------------------


# The top finite bucket of each histogram: a percentile at or above it only means "at least".
LATENCY_TOP = 60.0  # HTTP and fanout durations (feed_fanout/metrics.py _LATENCY)
POST_TO_FEED_TOP = 300.0


def ms(seconds: float | None, top: float = LATENCY_TOP) -> str:
    if seconds is None:
        return "n/a"
    return f"> {top:.0f} s" if seconds >= top else f"{seconds * 1000:.0f} ms"


def sec(seconds: float | None, top: float = POST_TO_FEED_TOP) -> str:
    if seconds is None:
        return "n/a"
    return f"> {top:.0f} s" if seconds >= top else f"{seconds:.2f} s"


def num(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.0f}"


def k6_ms(step: Step, endpoint: str, stat: str) -> str:
    value = step.k6.get(f"http_req_duration{{name:{endpoint}}}", {}).get(stat)
    return "n/a" if value is None else f"{value:.0f} ms"  # k6 reports milliseconds


def report(run: Path, steps: list[Step]) -> str:
    lines = [f"# Load test: {run.name}", "", "```", (run / "host.txt").read_text().strip(), "```", ""]
    knee = None
    for step in steps:
        if step.problems:
            break
        knee = step
    lines += [
        f"**Knee: {knee.rate} requests/s**, the last step the system sustained."
        if knee
        else "**Knee: below the first step**: even the first rate wasn't sustained.",
        "",
        "## Steps",
        "",
        "| Rate (req/s) | Sent (k6) | Served (server) | Failed (k6) | 5xx | Dropped | Posts fanned out "
        "| Post→feed p50 / p95 / p99 | Lag max (end) | Outbox age max | Sustainable |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for s in steps:
        reqs = s.k6.get("http_reqs", {})
        lines.append(
            f"| {s.rate} | {reqs.get('rate', 0):.0f}/s | {num(s.server.get('served_per_second'))}/s "
            f"| {s.k6.get('http_req_failed', {}).get('value', 0):.2%} | {num(s.server.get('server_5xx'))} "
            f"| {s.k6.get('dropped_iterations', {}).get('count', 0):.0f} | {num(s.server.get('posts_fanned_out'))} "
            f"| {sec(s.server.get('post_to_feed_p50'))} / {sec(s.server.get('post_to_feed_p95'))} / "
            f"{sec(s.server.get('post_to_feed_p99'))} "
            f"| {num(s.server.get('lag_max'))} ({num(s.server.get('lag_end'))}) "
            f"| {sec(s.server.get('outbox_age_max'))} | {'yes' if not s.problems else 'no'} |"
        )
    lines += ["", "## Latency per endpoint (p99: clients, from k6 / server, from Prometheus)", ""]
    lines += ["| Rate | " + " | ".join(ENDPOINTS) + " | Fanout p99 |", "|---" * (len(ENDPOINTS) + 2) + "|"]
    for s in steps:
        cells = [f"{k6_ms(s, e, 'p(99)')} / {ms(s.server.get(f'p99 {e}'))}" for e in ENDPOINTS]
        lines.append(f"| {s.rate} | " + " | ".join(cells) + f" | {ms(s.server.get('fanout_p99'))} |")
    lines += ["", "## Containers (average CPU, peak memory / limit)", ""]
    lines += ["| Rate | " + " | ".join(SERVICES) + " |", "|---" * (len(SERVICES) + 1) + "|"]
    for s in steps:
        cells = [
            f"{c[0]:.0f}%, {c[1]:.0f}/{c[2]:.0f} MiB" if (c := s.containers.get(svc)) else "n/a" for svc in SERVICES
        ]
        lines.append(f"| {s.rate} | " + " | ".join(cells) + " |")
    lines += ["", "## Why a step wasn't sustainable", ""]
    lines += [f"- **{s.rate}/s:** " + "; ".join(s.problems) for s in steps if s.problems] or ["- Every step was."]
    lines += [
        "",
        "- **Sent** is what k6 sent per second; **Served** is what the API answered, as it counted them.",
        "  The gap is requests that never got an answer the server could count (refused, or cut off).",
        "- Percentiles come from histogram buckets, so they're interpolated within a bucket, not exact;",
        '  "> 60 s" means at or past the top bucket (60 s for latencies, 300 s for post→feed).',
        "- CPU is % of one core (200% = two cores).",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    run = Path(sys.argv[1])
    steps = read_steps(run)
    for step in steps:
        read_server(step)
    read_containers(run, steps)
    judge(steps)
    print(report(run, steps))
    return 0


if __name__ == "__main__":
    sys.exit(main())
