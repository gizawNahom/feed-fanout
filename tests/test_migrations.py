"""The migrations build exactly the schema the models describe: nothing missing, nothing extra."""

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Engine

from feed_fanout.adapters.postgres.models import Base


def test_the_migrated_schema_matches_the_models(engine: Engine) -> None:
    with engine.connect() as conn:
        assert compare_metadata(MigrationContext.configure(conn), Base.metadata) == []
