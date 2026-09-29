"""
How fast a user may post: a token bucket. The bucket holds `burst` tokens, each post takes
one, and one comes back every `every`. So a user can post `burst` times in a row, then one
post per `every` on average.

The bucket is stored as a single timestamp, `full_at`: when it will be full again if the user
stops posting. Each post pushes it `every` further into the future; the bucket is empty when
it's `burst * every` ahead of now. (The same idea as GCRA.) No token counts, no fractions,
no background refill: just compare times.
"""

import math
from dataclasses import dataclass
from datetime import datetime, timedelta

from feed_fanout.domain.model import Rejected


class TooManyPosts(Rejected):
    def __init__(self, retry_after: timedelta) -> None:
        self.retry_after = retry_after
        super().__init__(f"too many posts: try again in {self.retry_after_seconds} seconds")

    @property
    def retry_after_seconds(self) -> int:
        """Whole seconds, rounded up: retrying after this long is guaranteed to be allowed."""
        return math.ceil(self.retry_after.total_seconds())


@dataclass(frozen=True)
class PostingLimit:
    burst: int
    every: timedelta

    def take(self, full_at: datetime | None, now: datetime) -> datetime:
        """Take a token for a post made `now`, from a bucket that's full again at `full_at`
        (None: never used). Returns the new `full_at`, or raises TooManyPosts if it's empty."""
        new_full_at = max(full_at or now, now) + self.every
        too_early_by = new_full_at - now - self.burst * self.every
        if too_early_by > timedelta(0):
            raise TooManyPosts(retry_after=too_early_by)
        return new_full_at


DEFAULT_POSTING_LIMIT = PostingLimit(burst=10, every=timedelta(seconds=6))  # 10 in a row, then 10 a minute
