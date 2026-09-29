"""
The domain: posts, follows and feeds, and the rules they obey. No I/O here, no framework:
everything in this package can be tested in microseconds.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime

MAX_USER_ID_LENGTH = 64
MAX_POST_LENGTH = 5000
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100


class Rejected(Exception):
    """A request that breaks a rule. Nothing was changed."""


class InvalidPost(Rejected):
    pass


class CannotFollowYourself(Rejected):
    pass


class InvalidPageSize(Rejected):
    pass


class InvalidCursor(Rejected):
    pass


@dataclass(frozen=True)
class Post:
    id: uuid.UUID
    author_id: str
    text: str
    created_at: datetime

    @classmethod
    def create(cls, author_id: str, text: str, now: datetime) -> "Post":
        if not 1 <= len(author_id) <= MAX_USER_ID_LENGTH:
            raise InvalidPost(f"author_id must be 1 to {MAX_USER_ID_LENGTH} characters")
        if not 1 <= len(text) <= MAX_POST_LENGTH:
            raise InvalidPost(f"text must be 1 to {MAX_POST_LENGTH} characters")
        return cls(id=uuid.uuid4(), author_id=author_id, text=text, created_at=now)


def check_can_follow(follower_id: str, followee_id: str) -> None:
    if follower_id == followee_id:
        raise CannotFollowYourself("users cannot follow themselves")


def check_page_size(limit: int) -> None:
    if not 1 <= limit <= MAX_PAGE_SIZE:
        raise InvalidPageSize(f"limit must be 1 to {MAX_PAGE_SIZE}")
