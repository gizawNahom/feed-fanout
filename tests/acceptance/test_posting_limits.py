"""Posting limits: nobody can flood their followers (or the fanout pipeline).

The default limit: 10 posts in a row, then one every 6 seconds (10 a minute).
"""

import pytest

from tests.acceptance.contract import Rejected, System, TooManyPosts


def test_a_burst_of_posts_is_allowed_then_the_author_must_wait(system: System) -> None:
    for i in range(10):
        system.post("alice", f"post {i}")

    with pytest.raises(TooManyPosts) as refused:
        system.post("alice", "one too many")
    assert refused.value.retry_after == 6


def test_after_waiting_the_author_can_post_again(system: System) -> None:
    for _ in range(10):
        system.post("alice")
    system.time_passes(seconds=6)

    system.post("alice")  # one post's worth came back...
    with pytest.raises(TooManyPosts):
        system.post("alice")  # ...and only one


def test_a_full_minute_of_waiting_allows_a_full_burst_again(system: System) -> None:
    for _ in range(10):
        system.post("alice")
    system.time_passes(seconds=60)

    for _ in range(10):
        system.post("alice")
    with pytest.raises(TooManyPosts):
        system.post("alice")


def test_waiting_longer_does_not_save_up_more_than_a_burst(system: System) -> None:
    for _ in range(5):
        system.post("alice")
    system.time_passes(seconds=3600)

    for _ in range(10):
        system.post("alice")
    with pytest.raises(TooManyPosts):
        system.post("alice")


def test_each_author_has_their_own_limit(system: System) -> None:
    for _ in range(10):
        system.post("alice")

    system.post("bob")


def test_refused_posts_never_reach_followers(system: System) -> None:
    system.follow("bob", "alice")
    accepted = [system.post("alice", f"post {i}") for i in range(10)]
    with pytest.raises(TooManyPosts):
        system.post("alice", "refused")
    system.settle()

    assert sorted(system.feed("bob").ids) == sorted(p.id for p in accepted)


def test_invalid_posts_do_not_count_against_the_limit(system: System) -> None:
    for _ in range(10):
        with pytest.raises(Rejected):
            system.post("alice", "")

    for _ in range(10):
        system.post("alice")
