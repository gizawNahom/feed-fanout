"""Contract: every metric the alert rules and the dashboard query must actually exist.

The alert unit tests (promtool) feed in series named by hand, so if a metric were renamed
in the code, they would all still pass, while in production the alert would silently never
fire and the dashboard panel would show "No data". This test closes that gap.
"""

import json
import re
from pathlib import Path

import pytest
from prometheus_client import REGISTRY
from sqlalchemy.orm import Session, sessionmaker

import feed_fanout.metrics  # noqa: F401  (registers the app's metrics)
from feed_fanout.adapters.kafka.lag import ConsumerGroupLagCollector
from feed_fanout.adapters.kafka.relay import OutboxCollector

OBSERVABILITY = Path(__file__).parent.parent / "observability"
RULES = OBSERVABILITY / "prometheus" / "rules.yml"
DASHBOARD = OBSERVABILITY / "grafana" / "dashboards" / "feed-fanout.json"

# App metric names in PromQL. Recording rules ("feed_fanout:...") use a colon, so they don't match.
METRIC_NAME = re.compile(r"\bfeed_fanout_[a-z0-9_]+")

# The names a metric family can be queried by, per type.
_SUFFIXES = {
    "counter": ["_total", "_created"],
    "histogram": ["_bucket", "_count", "_sum", "_created"],
    "gauge": [""],
}


@pytest.fixture
def known_names(session_factory: sessionmaker[Session], kafka_bootstrap: str) -> set[str]:
    """Every queryable name the app exposes.

    Taken from the registered metric families, not from samples: a labelled metric has no
    samples until it's first used (e.g. no dead letters yet), but it's still defined. The
    outbox gauges and the consumer group lag come from the relay's collectors, which are only
    registered in the relay process, so they're read here directly.
    """
    lag = ConsumerGroupLagCollector(kafka_bootstrap, "contract-test", "unused")
    families = [*REGISTRY.collect(), *OutboxCollector(session_factory).collect(), *lag.describe()]
    lag.close()
    return {f.name + suffix for f in families for suffix in _SUFFIXES.get(f.type, [""])}


def used_in_rules() -> set[str]:
    return set(METRIC_NAME.findall(RULES.read_text()))


def used_in_dashboard() -> set[str]:
    panels = json.loads(DASHBOARD.read_text())["panels"]
    exprs = [t["expr"] for p in panels for t in p.get("targets", [])]
    return set(METRIC_NAME.findall(" ".join(exprs)))


def test_alert_rules_only_use_metrics_that_exist(known_names: set[str]) -> None:
    used = used_in_rules()
    assert used, "found no metric names in the rules; is the pattern still right?"
    assert used - known_names == set(), "the alert rules query metrics the app doesn't expose"


def test_dashboard_only_uses_metrics_that_exist(known_names: set[str]) -> None:
    used = used_in_dashboard()
    assert used, "found no metric names in the dashboard; is the pattern still right?"
    assert used - known_names == set(), "the dashboard queries metrics the app doesn't expose"


def test_the_check_catches_a_renamed_metric(known_names: set[str]) -> None:
    # Guard against a check that passes vacuously: a stale name must be reported.
    query = "max(feed_fanout_outbox_oldest_event_age_seconds) > 60"  # the real one is ..._oldest_age_seconds

    assert set(METRIC_NAME.findall(query)) - known_names == {"feed_fanout_outbox_oldest_event_age_seconds"}
