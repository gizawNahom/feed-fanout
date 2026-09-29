"""The posting limit (a token bucket kept as one timestamp), at its exact boundaries."""

from datetime import UTC, datetime, timedelta

import pytest

from feed_fanout.domain.posting_limit import DEFAULT_POSTING_LIMIT, PostingLimit, TooManyPosts

NOW = datetime(2026, 1, 1, tzinfo=UTC)
LIMIT = PostingLimit(burst=3, every=timedelta(seconds=10))


def test_the_default_is_ten_in_a_row_then_ten_a_minute() -> None:
    assert PostingLimit(burst=10, every=timedelta(seconds=6)) == DEFAULT_POSTING_LIMIT


@pytest.mark.parametrize("full_at", [None, NOW, NOW - timedelta(days=1)])
def test_a_full_bucket_is_full_again_one_interval_after_a_post(full_at: datetime | None) -> None:
    assert LIMIT.take(full_at, NOW) == NOW + timedelta(seconds=10)


def test_each_post_pushes_full_at_one_interval_further() -> None:
    assert LIMIT.take(NOW + timedelta(seconds=7), NOW) == NOW + timedelta(seconds=17)


def test_the_last_token_can_be_taken() -> None:
    # Two posts made: one token left, and taking it empties the bucket exactly.
    assert LIMIT.take(NOW + timedelta(seconds=20), NOW) == NOW + timedelta(seconds=30)


def test_an_empty_bucket_refuses_and_says_how_long_until_a_token_is_back() -> None:
    with pytest.raises(TooManyPosts) as e:
        LIMIT.take(NOW + timedelta(seconds=30), NOW)

    assert e.value.retry_after == timedelta(seconds=10)
    assert str(e.value) == "too many posts: try again in 10 seconds"


def test_one_microsecond_short_of_a_token_is_still_refused() -> None:
    with pytest.raises(TooManyPosts) as e:
        LIMIT.take(NOW + timedelta(seconds=20, microseconds=1), NOW)

    assert e.value.retry_after == timedelta(microseconds=1)


@pytest.mark.parametrize(
    ("retry_after", "seconds"),
    [
        (timedelta(seconds=6), 6),
        (timedelta(seconds=5, microseconds=1), 6),  # rounded up: never tells a client to retry too early
        (timedelta(microseconds=1), 1),
    ],
)
def test_retry_after_is_in_whole_seconds_rounded_up(retry_after: timedelta, seconds: int) -> None:
    assert TooManyPosts(retry_after).retry_after_seconds == seconds
