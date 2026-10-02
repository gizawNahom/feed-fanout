"""
Settings, read from environment variables (or a .env file), with local-development defaults.

    KAFKA_BOOTSTRAP_SERVERS   default localhost:9092
    DATABASE_URL              required, except with ENVIRONMENT=dev (then the local database below)
    POST_CREATED_TOPIC        default post-created   (its dead-letter topic is <topic>.dlt)
    FANOUT_GROUP_ID           default fanout-workers
    LOG_LEVEL                 default INFO
    LOG_FORMAT                default auto  (json | text | auto: text on a terminal, JSON otherwise)
    ENVIRONMENT               default production  (dev | production)
    AUTH_ISSUER               default feed-fanout-dev   expected `iss` of access tokens
    AUTH_AUDIENCE             default feed-fanout-api   expected `aud` of access tokens
    AUTH_JWKS_URL             the identity provider's public keys (production)
    AUTH_DEV_PUBLIC_KEY_FILE  a local public key, accepted ONLY when ENVIRONMENT=dev
    POSTS_BURST               default 10   posts a user can make in a row...
    POSTS_PER_MINUTE          default 10   ...then this many a minute (429 beyond that)
    API_MAX_IN_FLIGHT         default 100  requests the API handles at once; beyond, 503 at once
    WORKER_HEALTH_PORT        default 8001  (0 disables; GET /health/live, /health/ready)
    RELAY_HEALTH_PORT         default 8002  (0 disables)

Values are validated at startup, so a typo like LOG_LEVEL=VERBOSE fails fast instead of
misbehaving later.
"""

from datetime import timedelta
from typing import Literal

from pydantic import PositiveFloat, PositiveInt, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from feed_fanout.domain.posting_limit import DEFAULT_POSTING_LIMIT, PostingLimit

DEV_DATABASE_URL = "postgresql+psycopg://feed:feed@localhost:5432/feed"  # docker-compose.yml's Postgres


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    database_url: str = ""  # required outside dev: see fill_dev_defaults
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

    posts_burst: PositiveInt = DEFAULT_POSTING_LIMIT.burst
    posts_per_minute: PositiveFloat = timedelta(minutes=1) / DEFAULT_POSTING_LIMIT.every

    # Overload protection (see ShedLoad in adapters/http.py). In flight = arrival rate x time per
    # request (Little's law): at ~300 requests/s per core, 100 in flight is at most ~0.3 s of
    # queuing, and far below the file descriptors that ran out in the first load test.
    api_max_in_flight: PositiveInt = 100

    @model_validator(mode="after")
    def fill_dev_defaults(self) -> "Settings":
        # The local database's well-known password must never be what production quietly uses:
        # a deploy that forgets DATABASE_URL fails at startup instead.
        if not self.database_url:
            if self.environment != "dev":
                raise ValueError("DATABASE_URL is required (a local default is only used with ENVIRONMENT=dev)")
            self.database_url = DEV_DATABASE_URL
        return self

    @model_validator(mode="after")
    def dev_key_only_in_dev(self) -> "Settings":
        # A single, non-rotating local key must never be trusted outside development.
        if self.auth_dev_public_key_file and self.environment != "dev":
            raise ValueError("AUTH_DEV_PUBLIC_KEY_FILE is only allowed with ENVIRONMENT=dev")
        return self

    @property
    def posting_limit(self) -> PostingLimit:
        return PostingLimit(burst=self.posts_burst, every=timedelta(minutes=1) / self.posts_per_minute)

    @property
    def post_created_dlt_topic(self) -> str:
        """Events that could not be processed."""
        return f"{self.post_created_topic}.dlt"


settings = Settings()
