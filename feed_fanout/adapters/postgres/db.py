from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from feed_fanout.config import settings

# pool_pre_ping: check a pooled connection is alive before using it (survives DB restarts).
# connect_timeout: fail fast instead of hanging when the database is unreachable (e.g. a
# network partition, where nothing refuses the connection), so health checks answer quickly.
engine = create_engine(settings.database_url, pool_pre_ping=True, connect_args={"connect_timeout": 5})
SessionLocal = sessionmaker(engine, expire_on_commit=False)
