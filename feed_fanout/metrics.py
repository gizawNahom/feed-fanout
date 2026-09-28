"""
Prometheus metrics, all defined in one place. Served at /metrics by every process:
the API (port 8000) and the worker and relay health servers (8001, 8002).

Label rule: labels only for small, fixed sets of values (route, outcome, reason). Never a
user_id or post_id: every distinct label value is a separate time series, and a million
users would take Prometheus down. Per-post detail belongs in the logs.
"""

from prometheus_client import Counter, Gauge, Histogram

# Latency buckets in seconds, from fast lookups to "something is badly wrong".
_LATENCY = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)

# --- API --------------------------------------------------------------------------

HTTP_REQUESTS = Counter(
    "feed_fanout_http_requests_total",
    "HTTP requests, by route template (not the raw path) and status code.",
    ["method", "route", "status"],
)
HTTP_DURATION = Histogram(
    "feed_fanout_http_request_duration_seconds",
    "HTTP request latency.",
    ["method", "route"],
    buckets=_LATENCY,
)

# --- Worker -----------------------------------------------------------------------

EVENTS = Counter(
    "feed_fanout_worker_events_total",
    "Events handled by the fanout worker, by outcome (fanned_out, dead_lettered, abandoned).",
    ["outcome"],
)
FANOUT_DURATION = Histogram(
    "feed_fanout_worker_fanout_duration_seconds",
    "Time to write one post into all its followers' feeds.",
    buckets=_LATENCY,
)
FEED_ROWS = Histogram(
    "feed_fanout_worker_feed_rows_written",
    "Feed rows written per event, i.e. the author's follower count. The tail shows celebrities.",
    buckets=(0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000),
)
POST_TO_FEED = Histogram(
    "feed_fanout_post_to_feed_seconds",
    "From a post's creation to it being in its followers' feeds: what users experience.",
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300),
)
RETRIES = Counter(
    "feed_fanout_worker_retries_total",
    "Failed fanout attempts that will be retried, by kind (transient, unexpected).",
    ["kind"],
)
DEAD_LETTERS = Counter(
    "feed_fanout_dead_letters_total",
    "Events moved to the dead-letter topic, by reason.",
    ["reason"],
)
CONSUMER_LAG = Gauge(
    "feed_fanout_worker_consumer_lag",
    "Messages not yet consumed, per partition this worker owns.",
    ["partition"],
)

# --- Relay ------------------------------------------------------------------------

RELAY_PUBLISHED = Counter("feed_fanout_relay_published_total", "Outbox events published to Kafka.")
RELAY_FAILED = Counter("feed_fanout_relay_publish_failures_total", "Outbox events whose publish failed (retried).")
# Outbox backlog and oldest-event age are read from the database at scrape time; see
# relay.OutboxCollector.
