"""
The memory assembly: the real domain rules and use cases, with in-memory adapters.
No Docker, no network, no database: each scenario runs in microseconds.
"""

from feed_fanout.application.use_cases import CreatePost, FanOutPost, Follow, ReadFeed, Unfollow
from feed_fanout.domain import model
from feed_fanout.domain.model import DEFAULT_PAGE_SIZE
from tests.acceptance.contract import FeedPage, Post, Rejected
from tests.fakes import InMemoryDatabase, TickingClock


def _post(p: model.Post) -> Post:
    return Post(id=str(p.id), author_id=p.author_id, text=p.text)


class MemorySystem:
    def __init__(self) -> None:
        self.db = InMemoryDatabase()
        uow = self.db.unit_of_work
        self._create_post = CreatePost(uow, TickingClock())
        self._follow = Follow(uow)
        self._unfollow = Unfollow(uow)
        self._read_feed = ReadFeed(uow)
        self._fan_out = FanOutPost(uow)

    def follow(self, user: str, followee: str) -> None:
        with _rejections():
            self._follow(user, followee)

    def unfollow(self, user: str, followee: str) -> None:
        with _rejections():
            self._unfollow(user, followee)

    def post(self, author: str, text: str = "hello") -> Post:
        with _rejections():
            return _post(self._create_post(author, text))

    def feed(self, user: str, limit: int = DEFAULT_PAGE_SIZE, cursor: str | None = None) -> FeedPage:
        with _rejections():
            page = self._read_feed(user, limit, cursor)
        return FeedPage(posts=[_post(p) for p in page.posts], next_cursor=page.next_cursor)

    def settle(self) -> None:
        """What the relay and the worker do, done directly: fan out every recorded event."""
        while self.db.state.outbox:
            event = self.db.state.outbox.pop(0)
            self._fan_out(event.post_id, event.author_id, event.created_at)


class _rejections:
    """Domain rejections become the contract's Rejected, as the HTTP API turns them into 4xx."""

    def __enter__(self) -> None:
        pass

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        if isinstance(exc, model.Rejected):
            raise Rejected(str(exc)) from exc
