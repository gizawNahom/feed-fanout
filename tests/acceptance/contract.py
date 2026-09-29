"""
The contract acceptance scenarios are written against: what the system does, in the
language of its users, with no mention of HTTP, SQL or Kafka.

Each "assembly" implements it with a different stack, and every scenario runs on each:
the same test proves the behavior at every level. (The idea: subsecond TDD,
https://github.com/subsecondtdd/todo-subsecond)
"""

from dataclasses import dataclass
from typing import Protocol


class Rejected(Exception):
    """The system refused an action (invalid input or a broken rule)."""


@dataclass(frozen=True)
class Post:
    id: str
    author_id: str
    text: str


@dataclass(frozen=True)
class FeedPage:
    posts: list[Post]
    next_cursor: str | None

    @property
    def ids(self) -> list[str]:
        return [p.id for p in self.posts]


class System(Protocol):
    def follow(self, user: str, followee: str) -> None: ...

    def unfollow(self, user: str, followee: str) -> None: ...

    def post(self, author: str, text: str = "hello") -> Post: ...

    def feed(self, user: str, limit: int = 20, cursor: str | None = None) -> FeedPage: ...

    def settle(self) -> None:
        """Wait until everything already accepted has reached followers' feeds.

        Fanout is asynchronous (eventually consistent), so scenarios call this before
        checking a feed. What "wait" means is up to each assembly.
        """
        ...
