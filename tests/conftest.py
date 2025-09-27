"""Shared test fixtures and configuration."""

import pytest


@pytest.fixture(autouse=False)
async def cleanup_db_connections():
    """Fixture to ensure database connections are cleaned up after test."""
    yield
    # Cleanup after test
    try:
        from app.db_postgres import cleanup_db
        await cleanup_db()
    except Exception:
        # Ignore cleanup errors
        pass


@pytest.fixture(autouse=True)
def suppress_connection_warnings():
    """Suppress SQLAlchemy connection warnings during tests."""
    import warnings
    
    # Filter out the specific warnings we're getting
    warnings.filterwarnings(
        "ignore",
        message="coroutine.*was never awaited",
        category=RuntimeWarning,
        module="sqlalchemy.*"
    )
    yield