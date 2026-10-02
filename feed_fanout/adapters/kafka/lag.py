"""
Consumer group lag as the broker sees it: per partition, how many events the fanout workers
haven't committed yet.

Measured from outside the workers on purpose. A worker reporting its own lag goes quiet
exactly when it matters: while it's stuck on one slow event, it can't update the metric either.
(The first load test showed this: a celebrity post blocked the only worker for 10+ seconds,
and its self-reported lag barely moved.) Read from the broker, a stuck or dead worker stops
committing, and the lag grows regardless.

Collected on each scrape, in the relay process, through a consumer that never subscribes: it
reads the group's committed offsets and the partitions' ends, but never joins the group, so it
can't take partitions away from the workers.

Why not the admin API's list_consumer_group_offsets: in confluent-kafka 2.15.1 it crashes the
whole process (a segfault in librdkafka) when the group doesn't exist yet, for example before
the workers' first commit. A metrics scrape must never be able to take the relay down.
"""

from collections.abc import Iterable

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from feed_fanout.logs import get_logger

log = get_logger(__name__)

NAME = "feed_fanout_consumer_group_lag"
HELP = (
    "Events not yet committed by the fanout workers' consumer group, per partition, as the broker "
    "sees it. Grows when workers are slow, stuck or gone."
)


class ConsumerGroupLagCollector(Collector):
    def __init__(self, bootstrap_servers: str, group: str, topic: str, timeout: float = 2.0) -> None:
        self.group = group
        self.topic = topic
        # Well under Prometheus's scrape timeout (5 s): if Kafka is down, the scrape still
        # returns the relay's other metrics, just without this one.
        self.timeout = timeout
        self.reader = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group, "enable.auto.commit": False})
        self.admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    def describe(self) -> Iterable[Metric]:
        # What it exposes, without calling Kafka (at registration, and in the metric contract test).
        return [GaugeMetricFamily(NAME, HELP, labels=["partition"])]

    def collect(self) -> Iterable[Metric]:
        try:
            lag = self.lag()
        except KafkaException as e:
            # Leave the gauge out rather than failing the whole scrape.
            log.warning("consumer group lag unavailable", error=str(e.args[0]) if e.args else repr(e))
            return
        family = GaugeMetricFamily(NAME, HELP, labels=["partition"])
        for partition, value in sorted(lag.items()):
            family.add_metric([str(partition)], value)
        yield family

    def lag(self) -> dict[int, int]:
        """Per partition: the newest offset minus the group's committed offset. A partition the
        group has never committed counts from its oldest retained event."""
        metadata = self.reader.list_topics(self.topic, timeout=self.timeout).topics[self.topic]
        if metadata.error is not None:
            raise KafkaException(metadata.error)
        partitions = [TopicPartition(self.topic, p) for p in sorted(metadata.partitions)]
        committed = self._committed(partitions)
        lag = {}
        for tp in partitions:
            oldest, newest = self.reader.get_watermark_offsets(tp, timeout=self.timeout)
            start = committed.get(tp.partition, -1)
            lag[tp.partition] = max(newest - (start if start >= 0 else oldest), 0)
        return lag

    def _committed(self, partitions: list[TopicPartition]) -> dict[int, int]:
        try:
            return {tp.partition: tp.offset for tp in self.reader.committed(partitions, timeout=self.timeout)}
        except KafkaException:
            # Kafka answers "not coordinator" for a group that has never existed (the workers
            # haven't started yet): nothing is committed. For a group that exists, it's a real error.
            if self._group_exists():
                raise
            return {}

    def _group_exists(self) -> bool:
        groups = self.admin.list_consumer_groups(request_timeout=self.timeout).result(timeout=self.timeout)
        return any(g.group_id == self.group for g in groups.valid)

    def close(self) -> None:
        self.reader.close()
