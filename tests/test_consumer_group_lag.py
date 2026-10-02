"""
Consumer group lag, measured from the broker (feed_fanout/adapters/kafka/lag.py).

The property that matters: lag reflects what the group has *committed*, whatever its consumers
are doing. A consumer that's stuck, or gone, stops committing, and the lag shows it.
"""

import uuid

from feed_fanout.adapters.kafka.lag import NAME, ConsumerGroupLagCollector
from feed_fanout.adapters.kafka.worker import make_consumer
from tests.test_fanout import produce_raw


def collector(bootstrap: str, group: str, topic: str) -> ConsumerGroupLagCollector:
    return ConsumerGroupLagCollector(bootstrap, group, topic, timeout=10)


def total(lag: dict[int, int]) -> int:
    return sum(lag.values())


def test_a_group_that_never_committed_lags_by_everything(kafka_bootstrap: str, topic: str) -> None:
    for _ in range(3):
        produce_raw(kafka_bootstrap, topic, key="alice", value=b"event")

    lag = collector(kafka_bootstrap, f"never-{uuid.uuid4().hex[:8]}", topic).lag()

    assert sorted(lag) == [0, 1, 2]  # every partition reported, even empty ones
    assert total(lag) == 3


def test_lag_follows_commits_not_consumption(kafka_bootstrap: str, topic: str) -> None:
    group = f"lag-{uuid.uuid4().hex[:8]}"
    for _ in range(3):
        produce_raw(kafka_bootstrap, topic, key="alice", value=b"event")
    consumer = make_consumer(kafka_bootstrap, group)
    consumer.subscribe([topic])
    try:
        first = None
        while first is None or first.error():
            first = consumer.poll(1.0)
        consumer.commit(message=first, asynchronous=False)
        lag = collector(kafka_bootstrap, group, topic)

        assert total(lag.lag()) == 2  # one committed, two to go

        # The consumer now has the other two in hand (fetched), but is "stuck": no commits.
        # A self-reported lag might already say 0. The broker's view still says 2, and grows.
        for _ in range(2):
            consumer.poll(1.0)
        produce_raw(kafka_bootstrap, topic, key="alice", value=b"event")

        assert total(lag.lag()) == 3
    finally:
        consumer.close()


def test_exposed_as_a_gauge_per_partition(kafka_bootstrap: str, topic: str) -> None:
    produce_raw(kafka_bootstrap, topic, key="alice", value=b"event")

    [family] = collector(kafka_bootstrap, f"never-{uuid.uuid4().hex[:8]}", topic).collect()

    assert family.name == NAME
    assert sorted(s.labels["partition"] for s in family.samples) == ["0", "1", "2"]
    assert sum(s.value for s in family.samples) == 1


def test_kafka_unreachable_leaves_the_metric_out_instead_of_failing_the_scrape() -> None:
    unreachable = ConsumerGroupLagCollector("127.0.0.1:1", "group", "topic", timeout=0.5)

    assert list(unreachable.collect()) == []
