"""Settings, with local-development defaults. Overridable via environment variables."""

import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+psycopg://feed:feed@localhost:5432/feed")

POST_CREATED_TOPIC = "post-created"
FANOUT_GROUP_ID = os.environ.get("FANOUT_GROUP_ID", "fanout-workers")
