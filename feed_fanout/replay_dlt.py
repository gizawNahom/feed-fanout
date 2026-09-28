"""
Replay dead letters: inspect the dead-letter topic, then send events back to their
original topic once the cause is fixed.

    uv run python -m feed_fanout.replay_dlt                          # dry run: list what's there
    uv run python -m feed_fanout.replay_dlt --reason unexpected --execute
    uv run python -m feed_fanout.replay_dlt --from-offset 3 --to-offset 7 --execute

Stateless: it selects dead letters by offset range and reason and never commits anything,
so a filtered run doesn't use up the dead letters it skipped. Replaying the same event
twice is harmless because fanout is idempotent. The dead-letter topic itself is never
modified.

Loop protection: each replay increments the dlt.replay_count header. If the event fails
again, it returns to the dead-letter topic carrying that count, and once it reaches
--max-replays the tool refuses to replay it again unless --force is given.
"""

import argparse
import sys
import uuid
from collections import Counter
from dataclasses import dataclass, field

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from feed_fanout.config import settings
from feed_fanout.kafka import REPLAY_COUNT_HEADER, Headers, close_producer, headers_of, make_producer


@dataclass(frozen=True)
class DeadLetter:
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes]]

    def header(self, name: str, default: str = "") -> str:
        for k, v in self.headers:
            if k == name:
                return v.decode(errors="replace")
        return default

    @property
    def reason(self) -> str:
        return self.header("dlt.reason", "?")

    @property
    def original_topic(self) -> str:
        return self.header("dlt.original.topic")

    @property
    def replay_count(self) -> int:
        return int(self.header(REPLAY_COUNT_HEADER, "0"))

    def describe(self) -> str:
        key = self.key.decode(errors="replace") if self.key else None
        return (
            f"partition {self.partition} offset {self.offset}  key={key}  reason={self.reason}  "
            f"replays={self.replay_count}  failed_at={self.header('dlt.failed_at')}\n"
            f"    error: {self.header('dlt.error')[:200]}"
        )


def read_dead_letters(
    bootstrap_servers: str,
    topic: str,
    from_offset: int = 0,
    to_offset: int | None = None,
) -> list[DeadLetter]:
    """Read dead letters from from_offset up to the end of each partition as of now."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"dlt-replay-{uuid.uuid4().hex[:8]}",  # required, but never committed
            "enable.auto.commit": False,
        }
    )
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        ends: dict[int, int] = {}
        assignment = []
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            start = max(low, from_offset)
            end = high if to_offset is None else min(high, to_offset + 1)
            if start < end:
                ends[p] = end
                assignment.append(TopicPartition(topic, p, start))
        consumer.assign(assignment)

        letters: list[DeadLetter] = []
        while ends:
            msg = consumer.poll(10)
            if msg is None:
                raise TimeoutError(f"timed out reading {topic}")
            if msg.error():
                raise RuntimeError(msg.error())
            p, offset = msg.partition(), msg.offset()
            if p is None or offset is None or p not in ends:
                continue
            if offset < ends[p]:
                letters.append(DeadLetter(p, offset, msg.key(), msg.value(), headers_of(msg)))
            if offset >= ends[p] - 1:
                del ends[p]
        return sorted(letters, key=lambda d: (d.partition, d.offset))
    finally:
        consumer.close()


@dataclass
class ReplayResult:
    replayed: list[DeadLetter] = field(default_factory=list)
    over_limit: list[DeadLetter] = field(default_factory=list)


def replay(
    letters: list[DeadLetter],
    producer: Producer,
    reasons: set[str] | None = None,
    max_replays: int = 3,
    force: bool = False,
    execute: bool = False,
) -> ReplayResult:
    """Send matching dead letters back to their original topic (only if execute=True)."""
    result = ReplayResult()
    errors: list[KafkaError] = []
    for d in letters:
        if reasons and d.reason not in reasons:
            continue
        if d.replay_count >= max_replays and not force:
            result.over_limit.append(d)
            continue
        result.replayed.append(d)
        if execute:
            headers: Headers = [(k, v) for k, v in d.headers if not k.startswith("dlt.")]
            headers.append((REPLAY_COUNT_HEADER, str(d.replay_count + 1).encode()))
            producer.produce(
                d.original_topic,
                key=d.key,
                value=d.value,
                headers=headers,
                on_delivery=lambda err, _: errors.append(err) if err else None,
            )
    if execute:
        undelivered = producer.flush(15)
        if undelivered or errors:
            raise RuntimeError(f"replay incomplete: {undelivered} undelivered, errors: {errors[:3]}")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--topic", default=settings.post_created_dlt_topic, help="dead-letter topic to read")
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--from-offset", type=int, default=0)
    parser.add_argument("--to-offset", type=int, default=None, help="inclusive")
    parser.add_argument("--reason", action="append", help="only this reason (repeatable)")
    parser.add_argument("--max-replays", type=int, default=3)
    parser.add_argument("--force", action="store_true", help="replay even past --max-replays")
    parser.add_argument("--execute", action="store_true", help="actually replay (default: dry run)")
    args = parser.parse_args(argv)

    letters = read_dead_letters(args.bootstrap_servers, args.topic, args.from_offset, args.to_offset)
    by_reason = Counter(d.reason for d in letters)
    print(
        f"{len(letters)} dead letter(s) in {args.topic}"
        + (f": {', '.join(f'{r}={n}' for r, n in by_reason.most_common())}" if letters else "")
    )

    producer = make_producer(args.bootstrap_servers)
    try:
        result = replay(letters, producer, set(args.reason or []), args.max_replays, args.force, args.execute)
    finally:
        close_producer(producer)

    verb = "replayed" if args.execute else "would replay"
    for d in result.replayed:
        print(f"{verb}: {d.describe()}")
    for d in result.over_limit:
        print(f"skipped (replayed {d.replay_count}x already, use --force): {d.describe()}")
    print(
        f"{len(result.replayed)} {verb}, {len(result.over_limit)} over the replay limit"
        + ("" if args.execute else "  (dry run: pass --execute to replay)")
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
