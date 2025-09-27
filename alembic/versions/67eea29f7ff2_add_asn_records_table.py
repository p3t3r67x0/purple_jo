"""Add ASN records table

Revision ID: 67eea29f7ff2
Revises: 202509271600
Create Date: 2025-09-27 15:12:17.640319

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '67eea29f7ff2'
down_revision = '202509271600'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create ASN records table or update existing table."""
    
    # Check if table exists
    conn = op.get_bind()
    result = conn.execute(sa.text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'asn_records'
        )
    """)).scalar()
    
    if not result:
        # Create ASN records table
        op.create_table(
            "asn_records",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("cidr", sa.String(length=64), nullable=False),
            sa.Column("asn", sa.String(length=32), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=False),
                      nullable=False, server_default=sa.text("NOW()")),
            sa.UniqueConstraint("cidr", "asn", name="uq_asn_records_cidr_asn"),
        )
    else:
        # Table exists, ensure proper constraints and indexes
        try:
            op.create_unique_constraint(
                "uq_asn_records_cidr_asn", "asn_records", ["cidr", "asn"]
            )
        except Exception:
            # Constraint might already exist
            pass
    
    # Create indexes for ASN records table (will be skipped if they exist)
    try:
        op.create_index("ix_asn_records_cidr", "asn_records", ["cidr"],
                        unique=False)
    except Exception:
        pass
    
    try:
        op.create_index("ix_asn_records_asn", "asn_records", ["asn"],
                        unique=False)
    except Exception:
        pass


def downgrade() -> None:
    """Drop ASN records table."""
    
    # Drop indexes
    op.drop_index("ix_asn_records_asn", table_name="asn_records")
    op.drop_index("ix_asn_records_cidr", table_name="asn_records")
    
    # Drop table
    op.drop_table("asn_records")
