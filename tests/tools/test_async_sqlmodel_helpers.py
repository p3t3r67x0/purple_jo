from tools.async_sqlmodel_helpers import asyncpg_pool_dsn


def test_asyncpg_pool_dsn_strips_async_driver():
    dsn = "postgresql+asyncpg://user:password@localhost/db"

    assert (
        asyncpg_pool_dsn(dsn)
        == "postgresql://user:password@localhost/db"
    )


def test_asyncpg_pool_dsn_preserves_plus_sign_in_password():
    dsn = "postgresql+asyncpg://user:pass+word@localhost/db"

    assert (
        asyncpg_pool_dsn(dsn)
        == "postgresql://user:pass+word@localhost/db"
    )
