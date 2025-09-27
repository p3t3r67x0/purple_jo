"""Add HTTP header columns to domains table

Revision ID: 202509271500
Revises: 202509271400
Create Date: 2025-09-27 15:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '202509271500'
down_revision = '202509271400'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add HTTP header columns to domains table."""

    # Add HTTP Response Header columns
    op.add_column(
        'domains',
        sa.Column(
            'header_content_type',
            sa.String(length=255),
            nullable=True
        )
    )
    op.add_column(
        'domains',
        sa.Column(
            'header_content_length',
            sa.Integer(),
            nullable=True
        )
    )
    op.add_column(
        'domains',
        sa.Column(
            'header_content_encoding',
            sa.String(length=50),
            nullable=True
        )
    )
    op.add_column(
        'domains',
        sa.Column(
            'header_cache_control',
            sa.String(length=255),
            nullable=True
        )
    )
    op.add_column(
        'domains',
        sa.Column(
            'header_etag',
            sa.String(length=255),
            nullable=True
        )
    )
    op.add_column(
        'domains',
        sa.Column(
            'header_set_cookie',
            sa.Text(),
            nullable=True
        )
    )
    op.add_column(
        'domains',
        sa.Column(
            'header_location',
            sa.String(length=2048),
            nullable=True
        )
    )
    op.add_column(
        'domains',
        sa.Column(
            'header_www_authenticate',
            sa.String(length=255),
            nullable=True
        )
    )
    op.add_column(
        'domains',
        sa.Column(
            'header_access_control_allow_origin',
            sa.String(length=255),
            nullable=True
        )
    )
    op.add_column(
        'domains',
        sa.Column(
            'header_strict_transport_security',
            sa.String(length=255),
            nullable=True
        )
    )


def downgrade() -> None:
    """Remove HTTP header columns from domains table."""

    # Remove HTTP Response Header columns
    op.drop_column('domains', 'header_strict_transport_security')
    op.drop_column('domains', 'header_access_control_allow_origin')
    op.drop_column('domains', 'header_www_authenticate')
    op.drop_column('domains', 'header_location')
    op.drop_column('domains', 'header_set_cookie')
    op.drop_column('domains', 'header_etag')
    op.drop_column('domains', 'header_cache_control')
    op.drop_column('domains', 'header_content_encoding')
    op.drop_column('domains', 'header_content_length')
    op.drop_column('domains', 'header_content_type')
