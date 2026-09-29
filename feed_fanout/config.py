"""
Settings, read from environment variables (or a .env file), with local-development defaults.

    KAFKA_BOOTSTRAP_SERVERS   default localhost:9092
    DATABASE_URL              default postgresql+psycopg://feed:feed@localhost:5432/feed
    POST_CREATED_TOPIC        default post-created   (its dead-letter topic is <topic>.dlt)
    FANOUT_GROUP_ID           default fanout-workers
    LOG_LEVEL                 default INFO
    LOG_FORMAT                default auto  (json | text | auto: text on a terminal, JSON otherwise)
    ENVIRONMENT               default production  (dev | production)
    AUTH_ISSUER               default feed-fanout-dev   expected `iss` of access tokens
    AUTH_AUDIENCE             default feed-fanout-api   expected `aud` of access tokens
    AUTH_JWKS_URL             the identity provider's public keys (production)
    AUTH_DEV_PUBLIC_KEY_FILE  a local public key, accepted ONLY when ENVIRONMENT=dev
    WORKER_HEALTH_PORT        default 8001  (0 disables; GET /health/live, /health/ready)
    RELAY_HEALTH_PORT         default 8002  (0 disables)

Values are validated at startup, so a typo like LOG_LEVEL=VERBOSE fails fast instead of
misbehaving later.
"""

from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    database_url: str = "postgresql+psycopg://feed:feed@localhost:5432/feed"
    post_created_topic: str = "post-created"
    fanout_group_id: str = "fanout-workers"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["auto", "json", "text"] = "auto"
    worker_health_port: int = 8001
    relay_health_port: int = 8002

    # Safe by default: anything not explicitly "dev" is treated as production.
    environment: Literal["dev", "production"] = "production"
    auth_issuer: str = "feed-fanout-dev"
    auth_audience: str = "feed-fanout-api"
    auth_jwks_url: str | None = None
    auth_dev_public_key_file: str | None = None

    @model_validator(mode="after")
    def dev_key_only_in_dev(self) -> "Settings":
        # A single, non-rotating local key must never be trusted outside development.
        if self.auth_dev_public_key_file and self.environment != "dev":
            raise ValueError("AUTH_DEV_PUBLIC_KEY_FILE is only allowed with ENVIRONMENT=dev")
        return self

    @property
    def post_created_dlt_topic(self) -> str:
        """Events that could not be processed."""
        return f"{self.post_created_topic}.dlt"


settings = Settings()
