"""merge performance indexes and service rename heads

Revision ID: ea1ed08fc60c
Revises: 202501042200, cca384c12cb2
Create Date: 2025-09-27 09:19:51.135658

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ea1ed08fc60c'
down_revision = ('202501042200', 'cca384c12cb2')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass