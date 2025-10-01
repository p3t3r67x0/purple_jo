# Agent Guidelines for `tools/`

## Scope
These instructions apply to every script inside the `tools/` directory tree. Follow them when adding new utilities or modifying existing ones.

## Architectural expectations
- Treat scripts as standalone entry points. Ensure they guard execution with `if __name__ == "__main__":` so helpers can be imported without side effects. Many existing modules expose reusable functions that other tools import.
- Prefer reusing shared helpers (`bootstrap.setup()`, `dns_shared`, etc.) instead of re-implementing bootstrap logic. Long-running services such as `dns_worker_service.py` rely on the shared bootstrap module to register logging, settings, and event-loop configuration; mirror that pattern when building related services.
- When a tool requires asynchronous database access, reach for `tools.async_sqlmodel_helpers`. It normalises DSNs, configures the async engine/session factory, and provides lifecycle helpers—avoid duplicating that wiring or importing the FastAPI app package just for database access.

## Database & external services
- Accept PostgreSQL DSNs via CLI flags or `POSTGRES_DSN`, then pass them through `resolve_async_dsn`/`asyncpg_pool_dsn`. The helpers already normalise sync DSNs to asyncpg-compatible variants and raise clear runtime errors on invalid input.
- Use the async session factories from `async_sqlmodel_helpers` (or `sqlmodel_helpers` for synchronous flows) instead of creating engines manually. These helpers enable pooling, pre-ping, and consistent exception handling across scripts.
- RabbitMQ-powered workers (for example `dns_worker_service.py`) should keep using `aio-pika` and the abstractions in `dns_shared.py` for runtime state, logging, and concurrency management.

## Coding conventions
- Stick to the standard `logging` module for diagnostics—avoid bare `print` statements. Service-style scripts already define module-level loggers and expect structured messages.
- Existing CLIs primarily use `click`; new commands should follow the same pattern unless there is a strong reason to use `argparse`.
- Keep imports sorted into stdlib/third-party/local groups and include `from __future__ import annotations` when forward references or typing benefits apply.
- Maintain line length around 100 characters to match the surrounding style. Type annotate public functions and coroutine signatures.

## Operational notes & script analysis
- The directory mixes ingestion (`import_domains.py`, `insert_asn.py`), enrichment (`extract_geoip.py`, `ssl_cert_scanner.py`), and long-running services (`dns_worker_service.py`, `dns_publisher_service.py`). Consult `tools/README.md` for a full inventory before adding new functionality so you can extend the appropriate workflow.
- Async helper modules (`async_sqlmodel_helpers.py`) expose `get_engine`, `get_session_factory`, and lifecycle utilities for disposing engines. Reuse these rather than instantiating `create_async_engine` directly.
- Service workers (for example `dns_worker_service.py`) rely on shared dataclasses (`WorkerSettings`) and runtime abstractions in `dns_shared.py` to coordinate concurrency, DNS timeouts, and record logging. Leverage those types when building sibling services to keep configuration consistent.
- Scripts that interact with external binaries or APIs (such as `masscan_scanner.py`, `screenshot_scraper.py`, `extract_certstream.py`) already document prerequisites within their module docstrings or the main README. Update those sections when behaviour changes so operators can understand dependencies.

