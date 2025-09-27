"""Add tables for contact messaging, rate limits, request stats, and subnet lookups."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "202501021310"
down_revision = "202501011200"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "contact_messages",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=100), nullable=True),
        sa.Column("email", sa.String(length=255), nullable=True),
        sa.Column("subject", sa.String(length=255), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "contact_rate_limits",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("ip_address", sa.String(length=64), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index(
        "ix_contact_rate_limits_ip_window",
        "contact_rate_limits",
        ["ip_address", "window_start"],
        unique=True,
    )

    op.create_table(
        "request_stats",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("path", sa.String(length=512), nullable=True),
        sa.Column("query", sa.String(length=512), nullable=True),
        sa.Column("fragment", sa.String(length=255), nullable=True),
        sa.Column("request_method", sa.String(length=16), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("remote_address", sa.String(length=64), nullable=True),
        sa.Column("scheme", sa.String(length=16), nullable=True),
        sa.Column("host", sa.String(length=255), nullable=True),
        sa.Column("port", sa.String(length=16), nullable=True),
        sa.Column("origin", sa.String(length=255), nullable=True),
        sa.Column("accept", sa.String(length=255), nullable=True),
        sa.Column("referer", sa.String(length=255), nullable=True),
        sa.Column("connection", sa.String(length=64), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("accept_language", sa.String(length=255), nullable=True),
        sa.Column("accept_encoding", sa.String(length=255), nullable=True),
    )
    op.create_index("ix_request_stats_created", "request_stats", ["created_at"], unique=False)
    op.create_index("ix_request_stats_path", "request_stats", ["path"], unique=False)

    op.create_table(
        "subnet_lookups",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=True),
        sa.Column("cidr", sa.String(length=64), nullable=False),
        sa.Column("ip_start", sa.String(length=64), nullable=True),
        sa.Column("ip_end", sa.String(length=64), nullable=True),
        sa.Column("asn", sa.String(length=32), nullable=True),
        sa.Column("asn_description", sa.Text(), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_subnet_lookups_cidr", "subnet_lookups", ["cidr"], unique=False)
    op.create_index("ix_subnet_lookups_asn", "subnet_lookups", ["asn"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_subnet_lookups_asn", table_name="subnet_lookups")
    op.drop_index("ix_subnet_lookups_cidr", table_name="subnet_lookups")
    op.drop_table("subnet_lookups")

    op.drop_index("ix_request_stats_path", table_name="request_stats")
    op.drop_index("ix_request_stats_created", table_name="request_stats")
    op.drop_table("request_stats")

    op.drop_index("ix_contact_rate_limits_ip_window", table_name="contact_rate_limits")
    op.drop_table("contact_rate_limits")

    op.drop_table("contact_messages")
