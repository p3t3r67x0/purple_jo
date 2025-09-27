"""Add performance indexes for txt_records table

Revision ID: 202509271400
Revises: 202509271300
Create Date: 2025-09-27 14:00:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '202509271400'
down_revision = '202509271300'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add performance indexes for txt_records table."""
    
    # TXT Records indexes
    op.create_index(
        "ix_txt_records_content_gin",
        "txt_records",
        ["content"],
        postgresql_using="gin",
        postgresql_ops={"content": "gin_trgm_ops"}
    )
    op.create_index(
        "ix_txt_records_domain_updated",
        "txt_records",
        ["domain_id", "updated_at"]
    )


def downgrade() -> None:
    """Remove performance indexes for txt_records table."""
    
    # TXT Records indexes
    op.drop_index("ix_txt_records_domain_updated", table_name="txt_records")
    op.drop_index("ix_txt_records_content_gin", table_name="txt_records")