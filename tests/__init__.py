import os

# Runs before any test module (and so before feed_fanout.config reads the settings). Production,
# the default environment, requires a DATABASE_URL. Tests build their own engines on throwaway
# containers, so this placeholder is never connected to: anything that used the app's global
# engine by mistake would fail loudly instead of touching a real database.
os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://unused:unused@unused.invalid:5432/unused")
