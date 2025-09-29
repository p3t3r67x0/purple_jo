"""Add image columns to domains table if missing

Revision ID: 8b3d42c7d90e
Revises: 67eea29f7ff2
Create Date: 2024-05-06 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8b3d42c7d90e"
down_revision = "67eea29f7ff2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Ensure domains.image columns exist."""

    op.execute(
        sa.text(
            "ALTER TABLE domains ADD COLUMN IF NOT EXISTS image VARCHAR(255)"
        )
    )
    op.execute(
        sa.text(
            """
            ALTER TABLE domains
            ADD COLUMN IF NOT EXISTS image_scan_failed TIMESTAMPTZ
            """
        )
    )


def downgrade() -> None:
    """Drop image columns from domains table."""

    op.execute(sa.text("ALTER TABLE domains DROP COLUMN IF EXISTS image"))
    op.execute(
        sa.text(
            "ALTER TABLE domains DROP COLUMN IF EXISTS image_scan_failed"
        )
    )
