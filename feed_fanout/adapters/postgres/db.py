from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from feed_fanout.config import settings

# pool_size + max_overflow=0: keep up to DB_POOL_SIZE connections open, and reuse them. The
# default (5, plus 10 "overflow" connections closed as soon as they're returned) churned under
# load: the API needs more than 5 at once, so it opened and closed connections constantly, and
# Postgres started 13,302 sessions in about three hours of load tests, a process and an
# authentication each. A request finding all connections busy waits in the pool instead, which
# costs nothing; overload protection (API_MAX_IN_FLIGHT) keeps that wait short.
# pool_pre_ping: check a pooled connection is alive before using it (survives DB restarts).
# connect_timeout: fail fast instead of hanging when the database is unreachable (e.g. a
# network partition, where nothing refuses the connection), so health checks answer quickly.
engine = create_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    max_overflow=0,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 5},
)
SessionLocal = sessionmaker(engine, expire_on_commit=False)
