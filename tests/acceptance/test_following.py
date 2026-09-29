"""Following: what a follow means for whose posts reach whose feed."""

import pytest

from tests.acceptance.contract import Rejected, System


def test_followers_receive_the_authors_posts(system: System) -> None:
    # Given bob and carol follow alice
    system.follow("bob", "alice")
    system.follow("carol", "alice")

    # When alice posts
    post = system.post("alice", "hello")
    system.settle()

    # Then both have the post in their feed
    assert system.feed("bob").ids == [post.id]
    assert system.feed("carol").ids == [post.id]


def test_people_who_dont_follow_the_author_dont_receive_the_post(system: System) -> None:
    system.follow("bob", "alice")
    system.follow("alice", "dave")  # the other direction: alice follows dave

    system.post("alice")
    system.settle()

    assert system.feed("dave").ids == []


def test_authors_dont_receive_their_own_posts(system: System) -> None:
    system.follow("bob", "alice")

    system.post("alice")
    system.settle()

    assert system.feed("alice").ids == []


def test_following_twice_is_the_same_as_following_once(system: System) -> None:
    system.follow("bob", "alice")
    system.follow("bob", "alice")

    post = system.post("alice")
    system.settle()

    assert system.feed("bob").ids == [post.id]


def test_you_cannot_follow_yourself(system: System) -> None:
    with pytest.raises(Rejected):
        system.follow("bob", "bob")


def test_posts_from_before_a_follow_are_not_backfilled(system: System) -> None:
    # Fanout-on-write: a post reaches the followers the author has when it's posted.
    system.post("alice")
    system.settle()

    system.follow("bob", "alice")

    assert system.feed("bob").ids == []


def test_after_unfollowing_new_posts_no_longer_arrive(system: System) -> None:
    system.follow("bob", "alice")
    before = system.post("alice", "before")
    system.settle()

    system.unfollow("bob", "alice")
    system.post("alice", "after")
    system.settle()

    # What's already in the feed stays; nothing new arrives.
    assert system.feed("bob").ids == [before.id]


def test_unfollowing_someone_you_dont_follow_is_harmless(system: System) -> None:
    system.unfollow("bob", "alice")
