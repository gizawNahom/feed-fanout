"""Event schemas published to Kafka."""

import uuid
from datetime import datetime

from pydantic import BaseModel


class PostCreated(BaseModel):
    """Published after a post is saved. Deliberately thin: the post itself lives in Postgres."""

    post_id: uuid.UUID
    author_id: str
    created_at: datetime
