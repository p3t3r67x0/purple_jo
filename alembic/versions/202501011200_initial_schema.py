"""Initial PostgreSQL schema generated from SQLModel definitions."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "202501011200"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "domains",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("country_code", sa.String(length=8), nullable=True),
        sa.Column("country", sa.String(length=255), nullable=True),
        sa.Column("state", sa.String(length=255), nullable=True),
        sa.Column("city", sa.String(length=255), nullable=True),
        sa.Column("banner", sa.Text(), nullable=True),
        sa.Column("header_status", sa.String(length=16), nullable=True),
        sa.Column("header_server", sa.String(length=255), nullable=True),
        sa.Column("header_x_powered_by", sa.String(length=255), nullable=True),
    )
    op.create_index("ix_domains_name", "domains", ["name"], unique=True)
    op.create_index("ix_domains_updated_at", "domains", ["updated_at"], unique=False)
    op.create_index("ix_domains_country_code", "domains", ["country_code"], unique=False)
    op.create_index("ix_domains_state", "domains", ["state"], unique=False)
    op.create_index("ix_domains_city", "domains", ["city"], unique=False)
    op.create_index("ix_domains_header_status", "domains", ["header_status"], unique=False)
    op.create_index("ix_domains_header_server", "domains", ["header_server"], unique=False)
    op.create_index("ix_domains_header_x_powered_by", "domains", ["header_x_powered_by"], unique=False)

    op.create_table(
        "whois_records",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("asn", sa.String(length=32), nullable=True),
        sa.Column("asn_description", sa.Text(), nullable=True),
        sa.Column("asn_country_code", sa.String(length=8), nullable=True),
        sa.Column("asn_registry", sa.String(length=32), nullable=True),
        sa.Column("asn_cidr", sa.String(length=64), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("domain_id"),
    )
    op.create_index("ix_whois_records_asn", "whois_records", ["asn"], unique=False)
    op.create_index("ix_whois_records_country_code", "whois_records", ["asn_country_code"], unique=False)
    op.create_index("ix_whois_records_updated_at", "whois_records", ["updated_at"], unique=False)

    op.create_table(
        "geo_points",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("country_code", sa.String(length=8), nullable=True),
        sa.Column("country", sa.String(length=255), nullable=True),
        sa.Column("state", sa.String(length=255), nullable=True),
        sa.Column("city", sa.String(length=255), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("domain_id"),
    )
    op.create_index("ix_geo_points_country_code", "geo_points", ["country_code"], unique=False)
    op.create_index("ix_geo_points_updated_at", "geo_points", ["updated_at"], unique=False)

    op.create_table(
        "ssl_data",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("issuer_common_name", sa.String(length=255), nullable=True),
        sa.Column("issuer_organization", sa.String(length=255), nullable=True),
        sa.Column("issuer_organizational_unit", sa.String(length=255), nullable=True),
        sa.Column("subject_common_name", sa.String(length=255), nullable=True),
        sa.Column("subject_organization", sa.String(length=255), nullable=True),
        sa.Column("subject_organizational_unit", sa.String(length=255), nullable=True),
        sa.Column("serial_number", sa.String(length=255), nullable=True),
        sa.Column("ocsp", sa.Text(), nullable=True),
        sa.Column("ca_issuers", sa.Text(), nullable=True),
        sa.Column("crl_distribution_points", sa.Text(), nullable=True),
        sa.Column("not_before", sa.DateTime(timezone=True), nullable=True),
        sa.Column("not_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("domain_id"),
    )
    op.create_index("ix_ssl_data_subject_cn", "ssl_data", ["subject_common_name"], unique=False)
    op.create_index("ix_ssl_data_issuer_cn", "ssl_data", ["issuer_common_name"], unique=False)
    op.create_index("ix_ssl_data_not_after", "ssl_data", ["not_after"], unique=False)
    op.create_index("ix_ssl_data_not_before", "ssl_data", ["not_before"], unique=False)
    op.create_index("ix_ssl_data_updated_at", "ssl_data", ["updated_at"], unique=False)

    op.create_table(
        "a_records",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("ip_address", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_a_records_domain_id", "a_records", ["domain_id"], unique=False)
    op.create_index("ix_a_records_ip", "a_records", ["ip_address"], unique=False)
    op.create_index("ix_a_records_updated_at", "a_records", ["updated_at"], unique=False)

    op.create_table(
        "aaaa_records",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("ip_address", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_aaaa_records_domain_id", "aaaa_records", ["domain_id"], unique=False)
    op.create_index("ix_aaaa_records_ip", "aaaa_records", ["ip_address"], unique=False)
    op.create_index("ix_aaaa_records_updated_at", "aaaa_records", ["updated_at"], unique=False)

    op.create_table(
        "cname_records",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("target", sa.String(length=255), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_cname_records_domain_id", "cname_records", ["domain_id"], unique=False)
    op.create_index("ix_cname_records_target", "cname_records", ["target"], unique=False)
    op.create_index("ix_cname_records_updated_at", "cname_records", ["updated_at"], unique=False)

    op.create_table(
        "mx_records",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.String(length=255), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_mx_records_domain_id", "mx_records", ["domain_id"], unique=False)
    op.create_index("ix_mx_records_exchange", "mx_records", ["exchange"], unique=False)
    op.create_index("ix_mx_records_priority", "mx_records", ["priority"], unique=False)
    op.create_index("ix_mx_records_updated_at", "mx_records", ["updated_at"], unique=False)

    op.create_table(
        "ns_records",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("value", sa.String(length=255), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_ns_records_domain_id", "ns_records", ["domain_id"], unique=False)
    op.create_index("ix_ns_records_value", "ns_records", ["value"], unique=False)
    op.create_index("ix_ns_records_updated_at", "ns_records", ["updated_at"], unique=False)

    op.create_table(
        "soa_records",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("value", sa.String(length=255), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_soa_records_domain_id", "soa_records", ["domain_id"], unique=False)
    op.create_index("ix_soa_records_value", "soa_records", ["value"], unique=False)
    op.create_index("ix_soa_records_updated_at", "soa_records", ["updated_at"], unique=False)

    op.create_table(
        "port_services",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("port", sa.Integer(), nullable=False),
        sa.Column("protocol", sa.String(length=16), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_port_services_domain_id", "port_services", ["domain_id"], unique=False)
    op.create_index("ix_port_services_port", "port_services", ["port"], unique=False)
    op.create_index("ix_port_services_status", "port_services", ["status"], unique=False)
    op.create_index("ix_port_services_updated_at", "port_services", ["updated_at"], unique=False)

    op.create_table(
        "ssl_subject_alt_names",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("ssl_id", sa.Integer(), nullable=False),
        sa.Column("value", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["ssl_id"], ["ssl_data.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_ssl_subject_alt_names_ssl_id", "ssl_subject_alt_names", ["ssl_id"], unique=False)
    op.create_index("ix_ssl_san_value", "ssl_subject_alt_names", ["value"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_ssl_san_value", table_name="ssl_subject_alt_names")
    op.drop_index("ix_ssl_subject_alt_names_ssl_id", table_name="ssl_subject_alt_names")
    op.drop_table("ssl_subject_alt_names")

    op.drop_index("ix_port_services_updated_at", table_name="port_services")
    op.drop_index("ix_port_services_status", table_name="port_services")
    op.drop_index("ix_port_services_port", table_name="port_services")
    op.drop_index("ix_port_services_domain_id", table_name="port_services")
    op.drop_table("port_services")

    op.drop_index("ix_ns_records_updated_at", table_name="ns_records")
    op.drop_index("ix_ns_records_value", table_name="ns_records")
    op.drop_index("ix_ns_records_domain_id", table_name="ns_records")
    op.drop_table("ns_records")

    op.drop_index("ix_soa_records_updated_at", table_name="soa_records")
    op.drop_index("ix_soa_records_value", table_name="soa_records")
    op.drop_index("ix_soa_records_domain_id", table_name="soa_records")
    op.drop_table("soa_records")

    op.drop_index("ix_mx_records_updated_at", table_name="mx_records")
    op.drop_index("ix_mx_records_priority", table_name="mx_records")
    op.drop_index("ix_mx_records_exchange", table_name="mx_records")
    op.drop_index("ix_mx_records_domain_id", table_name="mx_records")
    op.drop_table("mx_records")

    op.drop_index("ix_cname_records_updated_at", table_name="cname_records")
    op.drop_index("ix_cname_records_target", table_name="cname_records")
    op.drop_index("ix_cname_records_domain_id", table_name="cname_records")
    op.drop_table("cname_records")

    op.drop_index("ix_aaaa_records_updated_at", table_name="aaaa_records")
    op.drop_index("ix_aaaa_records_ip", table_name="aaaa_records")
    op.drop_index("ix_aaaa_records_domain_id", table_name="aaaa_records")
    op.drop_table("aaaa_records")

    op.drop_index("ix_a_records_updated_at", table_name="a_records")
    op.drop_index("ix_a_records_ip", table_name="a_records")
    op.drop_index("ix_a_records_domain_id", table_name="a_records")
    op.drop_table("a_records")

    op.drop_index("ix_ssl_data_updated_at", table_name="ssl_data")
    op.drop_index("ix_ssl_data_not_before", table_name="ssl_data")
    op.drop_index("ix_ssl_data_not_after", table_name="ssl_data")
    op.drop_index("ix_ssl_data_issuer_cn", table_name="ssl_data")
    op.drop_index("ix_ssl_data_subject_cn", table_name="ssl_data")
    op.drop_table("ssl_data")

    op.drop_index("ix_geo_points_updated_at", table_name="geo_points")
    op.drop_index("ix_geo_points_country_code", table_name="geo_points")
    op.drop_table("geo_points")

    op.drop_index("ix_whois_records_updated_at", table_name="whois_records")
    op.drop_index("ix_whois_records_country_code", table_name="whois_records")
    op.drop_index("ix_whois_records_asn", table_name="whois_records")
    op.drop_table("whois_records")

    op.drop_index("ix_domains_header_x_powered_by", table_name="domains")
    op.drop_index("ix_domains_header_server", table_name="domains")
    op.drop_index("ix_domains_header_status", table_name="domains")
    op.drop_index("ix_domains_city", table_name="domains")
    op.drop_index("ix_domains_state", table_name="domains")
    op.drop_index("ix_domains_country_code", table_name="domains")
    op.drop_index("ix_domains_updated_at", table_name="domains")
    op.drop_index("ix_domains_name", table_name="domains")
    op.drop_table("domains")
