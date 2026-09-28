"""
Settings, read from environment variables (or a .env file), with local-development defaults.

    KAFKA_BOOTSTRAP_SERVERS   default localhost:9092
    DATABASE_URL              default postgresql+psycopg://feed:feed@localhost:5432/feed
    POST_CREATED_TOPIC        default post-created   (its dead-letter topic is <topic>.dlt)
    FANOUT_GROUP_ID           default fanout-workers
    LOG_LEVEL                 default INFO

Values are validated at startup, so a typo like LOG_LEVEL=VERBOSE fails fast instead of
misbehaving later.
"""

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    database_url: str = "postgresql+psycopg://feed:feed@localhost:5432/feed"
    post_created_topic: str = "post-created"
    fanout_group_id: str = "fanout-workers"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @property
    def post_created_dlt_topic(self) -> str:
        """Events that could not be processed."""
        return f"{self.post_created_topic}.dlt"


settings = Settings()
