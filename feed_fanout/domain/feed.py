"""
Positions in a feed, and the cursor that carries one between pages.

A feed is ordered newest first by (created_at, post_id): the post id breaks ties between
posts created at the same instant, so every post has exactly one place in the order and
paging never skips or repeats one. That's keyset pagination: "the next page is everything
after this position", which stays correct while new posts arrive, unlike OFFSET.
"""

import base64
import binascii
import uuid
from dataclasses import dataclass
from datetime import datetime

from feed_fanout.domain.model import InvalidCursor, Post


@dataclass(frozen=True, order=True)
class FeedPosition:
    created_at: datetime
    post_id: uuid.UUID

    @classmethod
    def of(cls, post: Post) -> "FeedPosition":
        return cls(post.created_at, post.id)

    def to_cursor(self) -> str:
        """Opaque to clients: they pass it back, and never need to know what's inside."""
        return base64.urlsafe_b64encode(f"{self.created_at.isoformat()}|{self.post_id}".encode()).decode()

    @classmethod
    def from_cursor(cls, cursor: str) -> "FeedPosition":
        try:
            created_at, post_id = base64.urlsafe_b64decode(cursor.encode()).decode().split("|")
            return cls(datetime.fromisoformat(created_at), uuid.UUID(post_id))
        except (ValueError, binascii.Error, UnicodeDecodeError):
            raise InvalidCursor("invalid cursor") from None


@dataclass(frozen=True)
class FeedPage:
    posts: list[Post]
    next_cursor: str | None  # None: this is the last page
