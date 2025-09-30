"""Lowercase existing domain names

Revision ID: 9f7f6d1e2c9a
Revises: 8b3d42c7d90e
Create Date: 2024-05-07 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9f7f6d1e2c9a"
down_revision = "8b3d42c7d90e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Normalise existing domain names to lowercase."""

    conn = op.get_bind()

    # Remove duplicate domain records that would violate the lowercase unique constraint
    conn.execute(
        sa.text(
            """
            DELETE FROM domains
            WHERE id IN (
                SELECT id
                FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (PARTITION BY lower(name) ORDER BY id) AS rn
                    FROM domains
                ) ranked
                WHERE ranked.rn > 1
            )
            """
        )
    )

    # Normalise any remaining domains to lowercase
    conn.execute(sa.text("UPDATE domains SET name = lower(name)"))


def downgrade() -> None:
    """No-op downgrade for data normalisation."""
    # Nothing to do: we cannot recover the original casing once normalised.
    pass
