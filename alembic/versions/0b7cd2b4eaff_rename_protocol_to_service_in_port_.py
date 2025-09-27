"""rename_protocol_to_service_in_port_services

Revision ID: 0b7cd2b4eaff
Revises: 202501031000
Create Date: 2025-09-26 09:18:07.381068

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0b7cd2b4eaff'
down_revision = '202501031000'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Rename 'protocol' column to 'service' in port_services table
    op.alter_column('port_services', 'protocol', new_column_name='service')

    # Update column type to match model (VARCHAR(255) instead of VARCHAR(16))
    op.alter_column(
        'port_services',
        'service',
        type_=sa.String(length=255),
        existing_type=sa.String(length=16)
    )


def downgrade() -> None:
    # Revert column type change
    op.alter_column(
        'port_services',
        'service',
        type_=sa.String(length=16),
        existing_type=sa.String(length=255)
    )

    # Rename 'service' column back to 'protocol'
    op.alter_column('port_services', 'service', new_column_name='protocol')