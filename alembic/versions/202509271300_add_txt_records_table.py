"""Add txt_records table

Revision ID: 202509271300
Revises: ea1ed08fc60c
Create Date: 2025-09-27 13:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '202509271300'
down_revision = 'ea1ed08fc60c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add txt_records table."""
    op.create_table(
        'txt_records',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('domain_id', sa.Integer(), nullable=False),
        sa.Column('content', sa.String(length=1000), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['domain_id'], ['domains.id'], ),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    """Drop txt_records table."""
    op.drop_table('txt_records')