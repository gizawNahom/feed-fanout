"""
PostingBuckets contract: the same tests against the Postgres adapter and the in-memory fake,
plus what only Postgres can show: the limit holds when the same user posts concurrently.
"""

import threading
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.orm import Session, sessionmaker

from feed_fanout.adapters.postgres.repositories import PostgresUnitOfWork
from feed_fanout.application.ports import UnitOfWork
from feed_fanout.application.use_cases import CreatePost
from feed_fanout.domain.posting_limit import PostingLimit, TooManyPosts
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


def full_at(uow: UnitOfWorkFactory, user: str) -> datetime | None:
    with uow() as u:
        return u.posting_buckets.full_at(user)


def set_full_at(uow: UnitOfWorkFactory, user: str, value: datetime) -> None:
    with uow() as u:
        u.posting_buckets.full_at(user)
        u.posting_buckets.set_full_at(user, value)
        u.commit()


def test_a_user_who_never_posted_has_no_bucket_yet(uow: UnitOfWorkFactory) -> None:
    assert full_at(uow, "alice") is None


def test_full_at_is_kept_exactly(uow: UnitOfWorkFactory) -> None:
    value = T0 + timedelta(seconds=6, microseconds=123456)
    set_full_at(uow, "alice", value)

    assert full_at(uow, "alice") == value


def test_it_can_be_moved_forward(uow: UnitOfWorkFactory) -> None:
    set_full_at(uow, "alice", T0)
    set_full_at(uow, "alice", T0 + timedelta(seconds=6))

    assert full_at(uow, "alice") == T0 + timedelta(seconds=6)


def test_each_user_has_their_own(uow: UnitOfWorkFactory) -> None:
    set_full_at(uow, "alice", T0)

    assert full_at(uow, "bob") is None


def test_nothing_is_kept_without_commit(uow: UnitOfWorkFactory) -> None:
    set_full_at(uow, "alice", T0)
    with uow() as u:
        u.posting_buckets.full_at("alice")
        u.posting_buckets.set_full_at("alice", T0 + timedelta(seconds=6))
        # no commit

    assert full_at(uow, "alice") == T0


# --- Postgres only: concurrency -------------------------------------------------------


class FrozenClock:
    def now(self) -> datetime:
        return T0


@pytest.mark.parametrize("posted_before", [False, True], ids=["first posts", "later posts"])
def test_concurrent_posts_by_one_user_never_exceed_the_burst(
    session_factory: sessionmaker[Session], posted_before: bool
) -> None:
    # Like one user's requests spread over several API instances: each thread is its own
    # transaction. Time stands still, so exactly `burst` posts may succeed, however they
    # interleave. Without the row lock, several would read the same bucket and all succeed.
    # "first posts": the row doesn't exist yet, so the race is on creating it.
    limit = PostingLimit(burst=5, every=timedelta(seconds=6))
    create_post = CreatePost(lambda: PostgresUnitOfWork(session_factory, "unused"), FrozenClock(), limit)
    if posted_before:
        create_post("alice", "warm-up")
        limit_left = limit.burst - 1
    else:
        limit_left = limit.burst
    attempts = 12
    start = threading.Barrier(attempts)
    outcomes: list[str] = []

    def post() -> None:
        start.wait()
        try:
            create_post("alice", "hi")
            outcomes.append("posted")
        except TooManyPosts:
            outcomes.append("refused")

    threads = [threading.Thread(target=post) for _ in range(attempts)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert sorted(outcomes) == ["posted"] * limit_left + ["refused"] * (attempts - limit_left)
