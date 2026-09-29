"""Reading the feed: order and paging."""

import pytest

from tests.acceptance.contract import Rejected, System


def test_the_feed_is_newest_first(system: System) -> None:
    system.follow("bob", "alice")
    first, second, third = (system.post("alice", t) for t in ("first", "second", "third"))
    system.settle()

    assert system.feed("bob").ids == [third.id, second.id, first.id]


def test_an_empty_feed(system: System) -> None:
    page = system.feed("nobody")

    assert page.posts == []
    assert page.next_cursor is None


def test_paging_through_the_feed_returns_every_post_exactly_once(system: System) -> None:
    system.follow("bob", "alice")
    posts = [system.post("alice", f"post {i}") for i in range(7)]
    system.settle()

    pages, cursor = [], None
    while True:
        page = system.feed("bob", limit=3, cursor=cursor)
        pages.append(page.ids)
        if (cursor := page.next_cursor) is None:
            break

    assert [len(p) for p in pages] == [3, 3, 1]
    assert [i for page in pages for i in page] == [p.id for p in reversed(posts)]


def test_no_next_page_when_the_page_is_exactly_full(system: System) -> None:
    system.follow("bob", "alice")
    system.post("alice")
    system.post("alice")
    system.settle()

    page = system.feed("bob", limit=2)

    assert len(page.posts) == 2
    assert page.next_cursor is None


def test_a_malformed_cursor_is_rejected(system: System) -> None:
    with pytest.raises(Rejected):
        system.feed("bob", cursor="not-a-cursor")


@pytest.mark.parametrize("limit", [0, 101])
def test_page_size_is_bounded(system: System, limit: int) -> None:
    with pytest.raises(Rejected):
        system.feed("bob", limit=limit)


def test_the_largest_page_size_is_allowed(system: System) -> None:
    assert system.feed("bob", limit=100).posts == []
