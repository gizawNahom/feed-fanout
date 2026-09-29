"""
FeedRepository contract: the same tests run against the Postgres adapter and the in-memory
fake. The fast tests (memory assembly) are only as trustworthy as the fake, so this is what
keeps it honest: if it ever behaves differently from Postgres, a test here fails.

Setup goes through the same unit of work the use cases use (posts and follows are added
through their repositories), so this also covers the unit of work's commit/rollback.
"""

import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.adapters.postgres.repositories import PostgresUnitOfWork
from feed_fanout.application.ports import UnitOfWork, UnknownPost
from feed_fanout.domain.feed import FeedPosition
from feed_fanout.domain.model import Post
from tests.fakes import InMemoryDatabase

T0 = datetime(2026, 1, 1, tzinfo=UTC)

UnitOfWorkFactory = Callable[[], UnitOfWork]


@pytest.fixture(params=["memory", "postgres"])
def uow(request: pytest.FixtureRequest) -> Iterator[UnitOfWorkFactory]:
    if request.param == "memory":
        yield InMemoryDatabase().unit_of_work
    else:
        session_factory: sessionmaker[Session] = request.getfixturevalue("session_factory")
        yield lambda: PostgresUnitOfWork(session_factory, post_created_topic="unused")


def given_posts(uow: UnitOfWorkFactory, author: str, created_ats: list[datetime]) -> list[Post]:
    posts = [Post(uuid.uuid4(), author, "x", ts) for ts in created_ats]
    with uow() as u:
        for p in posts:
            u.posts.add(p)
        u.commit()
    return posts


def given_follows(uow: UnitOfWorkFactory, *pairs: tuple[str, str]) -> None:
    with uow() as u:
        for follower, followee in pairs:
            u.follows.add(follower, followee)
        u.commit()


def fan_out(uow: UnitOfWorkFactory, *posts: Post) -> list[int]:
    added = []
    with uow() as u:
        for p in posts:
            added.append(u.feeds.add_to_followers(p.id, p.author_id, p.created_at))
        u.commit()
    return added


def newest(uow: UnitOfWorkFactory, user: str, limit: int = 100, before: FeedPosition | None = None) -> list[Post]:
    with uow() as u:
        return u.feeds.newest(user, limit, before)


def ids(posts: list[Post]) -> list[uuid.UUID]:
    return [p.id for p in posts]


# --- Who receives a post ------------------------------------------------------------


def test_the_post_reaches_every_follower_and_no_one_else(uow: UnitOfWorkFactory) -> None:
    given_follows(uow, ("bob", "alice"), ("carol", "alice"), ("alice", "dave"))
    [post] = given_posts(uow, "alice", [T0])

    assert fan_out(uow, post) == [2]

    assert ids(newest(uow, "bob")) == [post.id]
    assert ids(newest(uow, "carol")) == [post.id]
    assert newest(uow, "dave") == []  # alice follows dave, not the other way round
    assert newest(uow, "alice") == []  # not the author


def test_fanning_out_twice_adds_nothing_the_second_time(uow: UnitOfWorkFactory) -> None:
    given_follows(uow, ("bob", "alice"))
    [post] = given_posts(uow, "alice", [T0])

    assert fan_out(uow, post, post) == [1, 0]
    assert ids(newest(uow, "bob")) == [post.id]


def test_following_twice_counts_once(uow: UnitOfWorkFactory) -> None:
    given_follows(uow, ("bob", "alice"), ("bob", "alice"))
    [post] = given_posts(uow, "alice", [T0])

    assert fan_out(uow, post) == [1]


def test_unfollowed_users_no_longer_receive_posts(uow: UnitOfWorkFactory) -> None:
    given_follows(uow, ("bob", "alice"))
    with uow() as u:
        u.follows.remove("bob", "alice")
        u.follows.remove("bob", "nobody")  # removing a follow that doesn't exist: no error
        u.commit()
    [post] = given_posts(uow, "alice", [T0])

    assert fan_out(uow, post) == [0]


def test_an_unknown_post_is_reported_when_there_are_followers(uow: UnitOfWorkFactory) -> None:
    given_follows(uow, ("bob", "alice"))

    with pytest.raises(UnknownPost), uow() as u:
        u.feeds.add_to_followers(uuid.uuid4(), "alice", T0)


def test_an_unknown_post_without_followers_goes_unnoticed(uow: UnitOfWorkFactory) -> None:
    # Nothing to insert, so nothing checks the post exists (Postgres's foreign key only
    # applies to rows being written). Harmless: there's no feed to put it in anyway.
    with uow() as u:
        assert u.feeds.add_to_followers(uuid.uuid4(), "alice", T0) == 0


# --- Order and paging ---------------------------------------------------------------


def test_newest_first(uow: UnitOfWorkFactory) -> None:
    given_follows(uow, ("bob", "alice"))
    posts = given_posts(uow, "alice", [T0 + timedelta(seconds=i) for i in (2, 0, 1)])
    fan_out(uow, *posts)

    assert [p.created_at for p in newest(uow, "bob")] == [T0 + timedelta(seconds=i) for i in (2, 1, 0)]


def test_identical_timestamps_are_ordered_by_post_id(uow: UnitOfWorkFactory) -> None:
    # Posts created at the same instant still have exactly one place each in the order,
    # or paging would skip or repeat them. The post id breaks the tie, the same way in both.
    given_follows(uow, ("bob", "alice"))
    posts = given_posts(uow, "alice", [T0] * 6)
    fan_out(uow, *posts)

    assert ids(newest(uow, "bob")) == sorted(ids(posts), reverse=True)


def test_paging_through_ties_returns_every_post_exactly_once(uow: UnitOfWorkFactory) -> None:
    given_follows(uow, ("bob", "alice"))
    posts = given_posts(uow, "alice", [T0] * 3 + [T0 + timedelta(seconds=1)] * 3 + [T0 - timedelta(seconds=1)])
    fan_out(uow, *posts)

    seen: list[uuid.UUID] = []
    before = None
    while page := newest(uow, "bob", limit=2, before=before):
        seen += ids(page)
        before = FeedPosition.of(page[-1])

    assert seen == ids(newest(uow, "bob"))  # same order as reading it all at once...
    assert sorted(seen) == sorted(ids(posts))  # ...and every post exactly once


def test_before_is_strictly_older(uow: UnitOfWorkFactory) -> None:
    given_follows(uow, ("bob", "alice"))
    first, second, third = given_posts(uow, "alice", [T0, T0 + timedelta(seconds=1), T0 + timedelta(seconds=2)])
    fan_out(uow, first, second, third)

    assert ids(newest(uow, "bob", before=FeedPosition.of(second))) == [first.id]


def test_limit(uow: UnitOfWorkFactory) -> None:
    given_follows(uow, ("bob", "alice"))
    posts = given_posts(uow, "alice", [T0 + timedelta(seconds=i) for i in range(5)])
    fan_out(uow, *posts)

    assert len(newest(uow, "bob", limit=2)) == 2


def test_an_empty_feed(uow: UnitOfWorkFactory) -> None:
    assert newest(uow, "nobody") == []


def test_posts_come_back_as_stored(uow: UnitOfWorkFactory) -> None:
    given_follows(uow, ("bob", "alice"))
    post = Post(uuid.uuid4(), "alice", "exactly this ✨", T0 + timedelta(microseconds=123456))
    with uow() as u:
        u.posts.add(post)
        u.commit()
    fan_out(uow, post)

    assert newest(uow, "bob") == [post]


# --- The unit of work ---------------------------------------------------------------


def test_nothing_is_kept_without_commit(uow: UnitOfWorkFactory) -> None:
    given_follows(uow, ("bob", "alice"))
    [post] = given_posts(uow, "alice", [T0])
    with uow() as u:
        u.feeds.add_to_followers(post.id, post.author_id, post.created_at)
        u.follows.add("carol", "alice")
        # no commit

    assert newest(uow, "bob") == []
    assert fan_out(uow, post) == [1]  # carol's follow wasn't kept either: only bob receives it
