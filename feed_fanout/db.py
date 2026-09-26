from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from feed_fanout.config import DATABASE_URL

# pool_pre_ping: check a pooled connection is alive before using it (survives DB restarts).
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, expire_on_commit=False)
