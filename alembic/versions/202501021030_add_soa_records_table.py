"""Add SoaRecord table for DNS data"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "202501021030"
down_revision = "202501011200"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if "soa_records" in inspector.get_table_names():
        return

    op.create_table(
        "soa_records",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), sa.ForeignKey("domains.id", ondelete="CASCADE"), nullable=False),
        sa.Column("value", sa.String(length=255), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_soa_records_domain_id", "soa_records", ["domain_id"], unique=False)
    op.create_index("ix_soa_records_value", "soa_records", ["value"], unique=False)
    op.create_index("ix_soa_records_updated_at", "soa_records", ["updated_at"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if "soa_records" not in inspector.get_table_names():
        return

    op.drop_index("ix_soa_records_updated_at", table_name="soa_records")
    op.drop_index("ix_soa_records_value", table_name="soa_records")
    op.drop_index("ix_soa_records_domain_id", table_name="soa_records")
    op.drop_table("soa_records")
