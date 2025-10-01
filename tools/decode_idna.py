#!/usr/bin/env python3

import asyncio
import idna
from datetime import datetime, timezone
from typing import List, Tuple

import asyncpg
import click

from idna.core import IDNAError

from async_sqlmodel_helpers import asyncpg_pool_dsn

try:
    from tool_runner import CLITool
except ModuleNotFoundError:
    from tools.tool_runner import CLITool
class IDNADecoder:
    """Decode IDNA-encoded domains stored in PostgreSQL."""

    @staticmethod
    def utcnow() -> datetime:
        """Return a naive UTC timestamp compatible with PostgreSQL columns."""

        return datetime.now(timezone.utc).replace(tzinfo=None)

    @classmethod
    async def create_postgres_pool(cls, dsn: str) -> asyncpg.Pool:
        """Create a PostgreSQL connection pool."""

        return await asyncpg.create_pool(asyncpg_pool_dsn(dsn))

    @classmethod
    async def retrieve_idna_domains(cls, pool: asyncpg.Pool) -> List[Tuple[int, str]]:
        """Retrieve domains containing IDNA encoding (xn--) from PostgreSQL."""

        async with pool.acquire() as conn:
            try:
                # Find domains that contain IDNA encoding patterns
                query = """
                    SELECT id, name
                    FROM domains
                    WHERE name ~ '(^|[.])(xn--)[a-z0-9]+'
                    ORDER BY id
                """
                rows = await conn.fetch(query)
                return [(row['id'], row['name']) for row in rows]
            except Exception as exc:  # noqa: BLE001
                print(f"[ERROR] Failed to retrieve IDNA domains: {exc}")
                return []

    @classmethod
    async def update_domain_name(
        cls,
        pool: asyncpg.Pool,
        domain_id: int,
        original_name: str,
        decoded_name: str,
    ) -> None:
        """Update domain name with decoded IDNA version."""

        async with pool.acquire() as conn:
            try:
                now = cls.utcnow()
                result = await conn.execute(
                    """
                    UPDATE domains
                    SET name = $1, updated_at = $2
                    WHERE id = $3
                    """,
                    decoded_name,
                    now,
                    domain_id,
                )

                if result == "UPDATE 1":
                    msg = f'INFO: Updated domain {original_name} -> {decoded_name}'
                    print(msg)
                else:
                    print(f'WARNING: Failed to update domain ID {domain_id}')
            except Exception as exc:  # noqa: BLE001
                print(f"ERROR: Failed to update domain {original_name}: {exc}")

    @classmethod
    async def process_idna_domains(cls, postgres_dsn: str) -> None:
        """Process IDNA domains and decode them."""

        pool = await cls.create_postgres_pool(postgres_dsn)

        try:
            # Retrieve domains with IDNA encoding
            domains = await cls.retrieve_idna_domains(pool)

            if not domains:
                click.echo("[INFO] No IDNA-encoded domains found")
                return

            click.echo(f"[INFO] Found {len(domains)} IDNA-encoded domains")

            decoded_count = 0
            for domain_id, idna_domain in domains:
                try:
                    # Attempt to decode IDNA domain
                    decoded_domain = idna.decode(idna_domain)

                    # Only update if decoding actually changed the domain
                    if decoded_domain and idna_domain != decoded_domain:
                        await cls.update_domain_name(
                            pool,
                            domain_id,
                            idna_domain,
                            decoded_domain,
                        )
                        decoded_count += 1

                except (UnicodeError, IDNAError) as exc:
                    print(f"WARNING: Failed to decode {idna_domain}: {exc}")
                    continue

            click.echo(f"[INFO] Successfully decoded {decoded_count} domains")

        finally:
            await pool.close()


@click.command()
@click.option(
    "--postgres-dsn",
    required=True,
    type=str,
    help="PostgreSQL DSN",
)
def main(postgres_dsn: str):
    """IDNA decoder tool with PostgreSQL backend.

    Finds domains with IDNA encoding (containing 'xn--') and decodes them
    to their Unicode representation, updating the database accordingly.
    """
    click.echo("[INFO] Starting IDNA domain decoding")

    asyncio.run(IDNADecoder.process_idna_domains(postgres_dsn))


if __name__ == '__main__':
    CLITool(main).run()
