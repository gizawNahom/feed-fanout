# One image for every process; docker-compose.yml picks the command:
#   api     uvicorn feed_fanout.adapters.http:app --host 0.0.0.0 --no-access-log
#   worker  python -m feed_fanout.adapters.kafka.worker
#   relay   python -m feed_fanout.adapters.kafka.relay
#   migrate alembic upgrade head

# --- Build: install dependencies into a virtualenv ---------------------------------
FROM python:3.14.8-slim-trixie AS build

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
FROM python:3.14.8-slim-trixie

# Debian's security updates, applied at build time. The official image is rebuilt only every so
# often, so it can lag behind fixes Debian has already published (OpenSSL, in 2026-10: the image
# scan in CI failed on it). Every build, and so every deploy, gets the patched packages.
RUN apt-get update \
    && apt-get upgrade --yes --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Run as an unprivileged user, never root. And no pip: the app runs from its virtualenv and
# never installs anything, and the base image's pip carries its own vulnerable vendored copies
# (found by the image scan). Less software, less to patch, less for an attacker to use.
# /keys: dev keys (local development only), owned by the app user.
RUN useradd --system --uid 10001 --no-create-home app \
    && install -d -o app /keys \
    && python -m pip uninstall --yes --root-user-action=ignore pip

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
#
# --no-access-log: no log line per HTTP request. Formatting one for every request cost CPU in
# the API's single process (under overload, for every request it turned away, too); metrics
# already count every request by route, status and latency, the app logs what happens (posts,
# follows, rejected tokens), and in production per-request logs come from the load balancer.
CMD ["uvicorn", "feed_fanout.adapters.http:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
