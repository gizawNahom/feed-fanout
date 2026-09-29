"""ReadFeed's contract with the repository: what it asks for, not only what it returns."""

from typing import Self

from feed_fanout.application.use_cases import ReadFeed
from feed_fanout.domain.feed import FeedPosition
from feed_fanout.domain.model import Post
from tests.fakes import InMemoryDatabase, InMemoryFeeds, InMemoryState, InMemoryUnitOfWork


class RecordingFeeds(InMemoryFeeds):
    def __init__(self, state: InMemoryState, requested_limits: list[int]) -> None:
        super().__init__(state)
        self.requested_limits = requested_limits

    def newest(self, user_id: str, limit: int, before: FeedPosition | None) -> list[Post]:
        self.requested_limits.append(limit)
        return super().newest(user_id, limit, before)


class RecordingUnitOfWork(InMemoryUnitOfWork):
    def __init__(self, db: InMemoryDatabase, requested_limits: list[int]) -> None:
        super().__init__(db)
        self.requested_limits = requested_limits

    def __enter__(self) -> Self:
        super().__enter__()
        self.feeds = RecordingFeeds(self.working, self.requested_limits)
        return self


def test_reads_only_one_more_row_than_the_page_size() -> None:
    # Behavior alone can't catch over-fetching: the page is trimmed to `limit` either way.
    # But against Postgres, asking for the whole feed would load every entry a user has
    # ever received, on every request. One extra row is all it takes to know whether a
    # next page exists.
    db = InMemoryDatabase()
    requested: list[int] = []

    ReadFeed(lambda: RecordingUnitOfWork(db, requested))("bob", limit=20)

    assert requested == [21]
