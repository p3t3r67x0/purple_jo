# Agent Guidelines for `purple_jo`

## Repository orientation
- This service is a FastAPI application that exposes network intelligence endpoints backed by PostgreSQL and Redis.
- Primary code lives under `app/`:
  - `app/main.py` wires FastAPI, middleware, and routers.
  - `app/routes/` contains lightweight route declarations; keep business logic in `app/api/` or `app/services/`.
  - `app/models/` defines SQLModel ORM models used by both the API and background tasks.
  - `app/db_postgres.py` owns engine/session setup—rely on its helpers instead of creating raw engines or sessions.
- `tests/` contains pytest-based test suites. Prefer mirroring the module layout when adding new coverage.
- `tools/` hosts standalone maintenance scripts that are not imported by the API at runtime.

## Coding conventions
- Target Python 3.10+. Use type annotations throughout. Import `from __future__ import annotations` in new modules when forward references are helpful.
- Follow the existing docstring style: add concise triple-quoted summaries for public functions, FastAPI route handlers, and tests.
- Stick to the standard logging module (`logging.getLogger(__name__)`) for diagnostics—do not introduce ad-hoc print debugging.
- When touching routes, keep endpoint parameter validation with FastAPI's dependency system (`Depends`, `Query`, etc.) and keep response schemas/types consistent with existing handlers.
- All database access must flow through `app.db_postgres.get_session`/`get_session_factory`. For request-scoped access use `Depends(get_postgres_session)` from `app.deps` instead of creating sessions manually.
- Prefer reusable helpers in `app.api` and `app.services` instead of embedding complex logic directly in routes.
- Keep line lengths around 100 characters to match the prevailing style, and format imports into logical groups (stdlib, third-party, local).

## Testing expectations
- Use `pytest` for all automated tests. Async tests should be decorated with `@pytest.mark.asyncio`.
- `pytest.ini` already scopes discovery to `tests/`. Place new fixtures in `tests/conftest.py` when they are shared.
- Integration tests that exercise PostgreSQL look for `POSTGRES_TEST_ADMIN_DSN` (or `POSTGRES_DSN`). If those variables are unset pytest will auto-skip, so document any new env requirements in the tests themselves.
- When adding cache-dependent logic, supply in-memory fakes (see `tests/test_cache.py`) so the suite remains deterministic without a running Redis instance.

## Operational notes
- Configuration should be obtained via `app.settings.get_settings()` or the constants exported from `app.config`; avoid reading environment variables directly from feature code.
- If you add new models or migrations, ensure Alembic metadata (`alembic/`) stays in sync and document manual steps in the migration guides when necessary.
- Background tasks or scripts in `tools/` should guard network access and wrap top-level logic in `if __name__ == "__main__":` blocks to avoid side effects on import.

