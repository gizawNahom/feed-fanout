"""Posting: what a post must look like."""

import pytest

from tests.acceptance.contract import Rejected, System


def test_a_post_is_returned_as_created(system: System) -> None:
    post = system.post("alice", "hello world")

    assert post.author_id == "alice"
    assert post.text == "hello world"
    assert post.id


def test_followers_see_the_post_exactly_as_written(system: System) -> None:
    system.follow("bob", "alice")

    system.post("alice", "exactly this ✨")
    system.settle()

    [seen] = system.feed("bob").posts
    assert (seen.author_id, seen.text) == ("alice", "exactly this ✨")


def test_the_longest_allowed_post_is_accepted(system: System) -> None:
    assert len(system.post("alice", "x" * 5000).text) == 5000


@pytest.mark.parametrize(
    ("author", "text"),
    [
        ("alice", ""),  # nothing to say
        ("alice", "x" * 5001),  # too long
        ("", "hello"),  # no author
        ("a" * 65, "hello"),  # author id too long
    ],
)
def test_invalid_posts_are_rejected(system: System, author: str, text: str) -> None:
    with pytest.raises(Rejected):
        system.post(author, text)
