"""Backoff timing. Pure unit tests: no containers."""

import random

from feed_fanout.worker import RetryPolicy

POLICY = RetryPolicy(base_delay=1.0, max_delay=30.0)


def test_ceiling_doubles_each_attempt_up_to_the_cap() -> None:
    # rand() -> just under 1 gives (almost) the ceiling itself.
    ceilings = [POLICY.delay(n, rand=lambda: 0.999999) for n in range(1, 8)]

    assert [round(c) for c in ceilings] == [1, 2, 4, 8, 16, 30, 30]


def test_delay_is_randomized_below_the_ceiling() -> None:
    rng = random.Random(42)
    delays = [POLICY.delay(4, rand=rng.random) for _ in range(1000)]

    assert all(0 <= d < 8 for d in delays)
    # Spread across the range instead of synchronized on one value.
    assert min(delays) < 1 and max(delays) > 7
    assert len({round(d, 3) for d in delays}) > 900
