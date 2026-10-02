"""Settings guards: production never runs on development defaults."""

import pytest

from feed_fanout.config import DEV_DATABASE_URL, Settings


def test_production_requires_a_database_url() -> None:
    with pytest.raises(ValueError, match="DATABASE_URL is required"):
        Settings(environment="production", database_url="")


def test_dev_uses_the_local_database_by_default() -> None:
    assert Settings(environment="dev", database_url="").database_url == DEV_DATABASE_URL


def test_a_configured_database_url_is_used_as_is() -> None:
    url = "postgresql+psycopg://app@db.internal:5432/feed"

    assert Settings(environment="production", database_url=url).database_url == url


def test_database_connections_are_kept_and_reused() -> None:
    # No "overflow" connections: those are closed as soon as they're returned, which churned
    # under load (thousands of Postgres sessions). Busy means waiting for a pooled one instead.
    from feed_fanout.adapters.postgres.db import engine
    from feed_fanout.config import settings

    assert engine.pool.size() == settings.db_pool_size  # type: ignore[attr-defined]
    assert engine.pool._max_overflow == 0  # type: ignore[attr-defined]
