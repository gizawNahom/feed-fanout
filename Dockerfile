# One image for every process; docker-compose.yml picks the command:
#   api     uvicorn feed_fanout.adapters.http:app --host 0.0.0.0
#   worker  python -m feed_fanout.adapters.kafka.worker
#   relay   python -m feed_fanout.adapters.kafka.relay
#   migrate alembic upgrade head

# --- Build: install dependencies into a virtualenv ---------------------------------
FROM python:3.14.7-slim-trixie AS build

COPY --from=ghcr.io/astral-sh/uv:0.12.1 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app
# Dependencies first, in their own layer: code changes don't reinstall them.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project

# --- Runtime: just Python, the virtualenv and the code ------------------------------
FROM python:3.14.7-slim-trixie

# Run as an unprivileged user, never root.
RUN useradd --system --uid 10001 --no-create-home app

WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY alembic.ini ./
COPY migrations ./migrations
COPY feed_fanout ./feed_fanout

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_FORMAT=json

USER app

# Exec form (no shell), so the process is PID 1 and receives SIGTERM directly:
# docker stop triggers the graceful shutdown instead of a kill after the timeout.
CMD ["uvicorn", "feed_fanout.adapters.http:app", "--host", "0.0.0.0", "--port", "8000"]
