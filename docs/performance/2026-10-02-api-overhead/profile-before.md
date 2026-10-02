# Profile: profile-20261002-160802.speedscope.json

4322 samples (~43 s of CPU) across 42 thread(s), each taken while a thread held the GIL.

## Self time by library: where CPU is burned

| Library | Share of CPU |
|---|---|
| `anyio` | 21.6% |
| `starlette` | 14.8% |
| `sqlalchemy` | 11.3% |
| `stdlib: asyncio` | 10.7% |
| `fastapi` | 10.1% |
| `psycopg` | 5.9% |
| `uvicorn` | 3.7% |
| `stdlib: threading` | 3.3% |
| `pydantic` | 2.8% |
| `<string>` | 1.9% |
| `stdlib: logging` | 1.9% |
| `feed_fanout (our code)` | 1.7% |
| `stdlib: weakref` | 1.5% |
| `stdlib: contextlib` | 1.5% |
| `structlog` | 1.2% |

## Total time by library: CPU spent in or under its code

| Library | Share of CPU |
|---|---|
| `anyio` | 81.1% |
| `click` | 77.3% |
| `/app/.venv/bin/uvicorn` | 77.3% |
| `uvicorn` | 77.3% |
| `stdlib: asyncio` | 77.3% |
| `starlette` | 67.1% |
| `fastapi` | 53.5% |
| `feed_fanout (our code)` | 33.5% |
| `stdlib: threading` | 25.9% |
| `sqlalchemy` | 18.3% |
| `psycopg` | 6.5% |
| `stdlib: contextlib` | 4.2% |
| `stdlib: logging` | 4.0% |
| `stdlib: queue` | 3.3% |
| `pydantic` | 2.9% |

## Self time by function

| Function | Share of CPU |
|---|---|
| `run (asyncio/runners.py)` | 5.8% |
| `wrapped_app (starlette/_exception_handler.py)` | 3.4% |
| `app (fastapi/routing.py)` | 3.1% |
| `__init__ (anyio/_backends/_asyncio.py)` | 2.8% |
| `notify (threading.py)` | 2.7% |
| `solve_dependencies (fastapi/dependencies/utils.py)` | 2.7% |
| `_spawn (anyio/_backends/_asyncio.py)` | 2.7% |
| `__call__ (starlette/applications.py)` | 1.8% |
| `__call__ (starlette/middleware/base.py)` | 1.7% |
| `__init__ (<string>)` | 1.6% |
| `dump_json (pydantic/type_adapter.py)` | 1.5% |
| `run_sync_in_worker_thread (anyio/_backends/_asyncio.py)` | 1.5% |
| `__init__ (asyncio/locks.py)` | 1.4% |
| `receive (anyio/streams/memory.py)` | 1.3% |
| `__init__ (sqlalchemy/orm/state.py)` | 1.3% |
| `wait (asyncio/locks.py)` | 1.1% |
| `__enter__ (anyio/_backends/_asyncio.py)` | 0.9% |
| `coro (starlette/middleware/base.py)` | 0.9% |
| `current_async_library (anyio/_core/_eventloop.py)` | 0.9% |
| `send (uvicorn/protocols/http/httptools_impl.py)` | 0.9% |
| `__new__ (anyio/_core/_streams.py)` | 0.8% |
| `call_next (starlette/middleware/base.py)` | 0.8% |
| `_generate_cache_key (sqlalchemy/sql/cache_key.py)` | 0.8% |
| `__getitem__ (weakref.py)` | 0.8% |
| `set (asyncio/locks.py)` | 0.8% |

## Total time by function

| Function | Share of CPU |
|---|---|
| `run (asyncio/runners.py)` | 77.3% |
| `run (uvicorn/main.py)` | 77.3% |
| `invoke (click/core.py)` | 77.3% |
| `main (click/core.py)` | 77.3% |
| `main (uvicorn/main.py)` | 77.3% |
| `<module> (.venv/bin/uvicorn)` | 77.3% |
| `__call__ (click/core.py)` | 77.3% |
| `run (uvicorn/server.py)` | 77.3% |
| `_run_coro (anyio/_core/_tasks.py)` | 49.8% |
| `coro (starlette/middleware/base.py)` | 48.8% |
| `__call__ (starlette/middleware/exceptions.py)` | 36.0% |
| `wrapped_app (starlette/_exception_handler.py)` | 35.4% |
| `__call__ (fastapi/middleware/asyncexitstack.py)` | 35.1% |
| `__call__ (starlette/routing.py)` | 34.9% |
| `app (fastapi/routing.py)` | 34.7% |
| `handle (fastapi/routing.py)` | 33.5% |
| `handle (starlette/routing.py)` | 33.3% |
| `__call__ (starlette/middleware/base.py)` | 27.8% |
| `_bootstrap_inner (threading.py)` | 22.7% |
| `_bootstrap (threading.py)` | 22.7% |
| `run (anyio/_backends/_asyncio.py)` | 22.7% |
| `__call__ (feed_fanout/application/use_cases.py)` | 20.0% |
| `run_asgi (uvicorn/protocols/http/httptools_impl.py)` | 19.0% |
| `__call__ (uvicorn/middleware/proxy_headers.py)` | 18.8% |
| `__call__ (fastapi/applications.py)` | 18.5% |

