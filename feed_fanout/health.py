"""
Health checks for the background processes (worker, relay), over a tiny HTTP server.

    GET /health/live   200 if the main loop is making progress, else 503 -> orchestrator restarts
    GET /health/ready  200 if the process should be doing work now, else 503 -> no restart

Liveness deliberately ignores dependencies: if Postgres is down, restarting every worker
can't fix it and only adds a restart storm. A worker waiting out an outage is alive.
Consumer lag isn't a health signal either (a slow worker is alive); it belongs in metrics.

The HTTP server runs on a daemon thread; v6 will serve Prometheus metrics from it too.
"""

import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Health:
    live: bool
    ready: bool
    details: dict[str, Any] = field(default_factory=dict)


class HealthCheck(Protocol):
    def health(self) -> Health: ...


class Heartbeat:
    """Progress tracking for a main loop. Written by the loop's thread, read by the health server."""

    def __init__(self, stale_after: float = 60.0, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self.stale_after = stale_after  # no beat for this long = stuck
        self.state = "starting"
        self.consecutive_failures = 0
        self._last_beat = clock()
        self._last_success: float | None = None

    def beat(self, state: str | None = None) -> None:
        """Call on every loop iteration, including while waiting or backing off."""
        self._last_beat = self.clock()
        if state is not None:
            self.state = state

    def success(self) -> None:
        self._last_success = self.clock()
        self.consecutive_failures = 0

    def failure(self) -> None:
        self.consecutive_failures += 1

    def live(self) -> bool:
        return self.clock() - self._last_beat < self.stale_after

    def details(self) -> dict[str, Any]:
        now = self.clock()
        return {
            "state": self.state,
            "consecutive_failures": self.consecutive_failures,
            "last_loop_seconds_ago": round(now - self._last_beat, 1),
            "last_success_seconds_ago": None if self._last_success is None else round(now - self._last_success, 1),
        }


class HealthServer:
    def __init__(self, check: HealthCheck, port: int, host: str = "0.0.0.0") -> None:
        self._server = ThreadingHTTPServer((host, port), _handler_for(check))
        self._thread = threading.Thread(target=self._server.serve_forever, name="health", daemon=True)

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    def start(self) -> "HealthServer":
        self._thread.start()
        log.info("health checks on :%d/health/live and /health/ready", self.port)
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def start_health_server(check: HealthCheck, port: int) -> HealthServer | None:
    """Start serving health checks, or return None if port is 0 (disabled) or already taken."""
    if port == 0:
        return None
    try:
        return HealthServer(check, port).start()
    except OSError as e:
        # e.g. a second worker on the same machine: keep working, just without health checks.
        log.warning("health checks disabled: can't listen on port %d (%s); set a different port", port, e)
        return None


def _handler_for(check: HealthCheck) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path not in ("/health/live", "/health/ready"):
                self._send(404, {"error": "not found"})
                return
            health = check.health()
            ok = health.live if self.path == "/health/live" else health.ready
            body = {"status": "ok" if ok else "unavailable", "live": health.live, "ready": health.ready}
            self._send(200 if ok else 503, body | health.details)

        def _send(self, code: int, body: dict[str, Any]) -> None:
            payload = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: Any) -> None:
            pass  # probes hit this every few seconds; don't flood the logs

    return Handler
