"""Consolidated PostgreSQL schema with all changes

Revision ID: 202509271600
Revises: None
Create Date: 2025-09-27 16:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '202509271600'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create complete PostgreSQL schema with all features."""
    
    # Create domains table with all columns
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
        # HTTP Response Headers
        sa.Column("header_content_type", sa.String(length=255), nullable=True),
        sa.Column("header_content_length", sa.Integer(), nullable=True),
        sa.Column("header_content_encoding", sa.String(length=50), nullable=True),
        sa.Column("header_cache_control", sa.String(length=255), nullable=True),
        sa.Column("header_etag", sa.String(length=255), nullable=True),
        sa.Column("header_set_cookie", sa.Text(), nullable=True),
        sa.Column("header_location", sa.String(length=2048), nullable=True),
        sa.Column("header_www_authenticate", sa.String(length=255), nullable=True),
        sa.Column("header_access_control_allow_origin", sa.String(length=255), nullable=True),
        sa.Column("header_strict_transport_security", sa.String(length=255), nullable=True),
        # QR Code column
        sa.Column("qrcode", sa.Text(), nullable=True),
        sa.Column("image", sa.String(length=255), nullable=True),
        sa.Column("image_scan_failed", sa.DateTime(timezone=True), nullable=True),
    )
    
    # Create indexes for domains table
    op.create_index("ix_domains_name", "domains", ["name"], unique=True)
    op.create_index("ix_domains_city_updated", "domains", ["city", "updated_at"], unique=False)
    op.create_index("ix_domains_country_updated", "domains", ["country", "updated_at"], unique=False)
    op.create_index("ix_domains_state_updated", "domains", ["state", "updated_at"], unique=False)
    op.create_index("ix_domains_country_code_updated", "domains", ["country_code", "updated_at"], unique=False)
    
    # Create A records table
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

    # Create AAAA records table
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

    # Create NS records table
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

    # Create SOA records table
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

    # Create MX records table
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

    # Create CNAME records table
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

    # Create TXT records table
    op.create_table(
        "txt_records",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("content", sa.String(length=1000), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_txt_records_domain_id", "txt_records", ["domain_id"], unique=False)
    op.create_index("ix_txt_records_updated_at", "txt_records", ["updated_at"], unique=False)

    # Create port services table (using 'service' instead of 'protocol')
    op.create_table(
        "port_services",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("port", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=True),
        sa.Column("service", sa.String(length=255), nullable=True),  # Updated name and length
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_port_services_domain_id", "port_services", ["domain_id"], unique=False)
    op.create_index("ix_port_services_port", "port_services", ["port"], unique=False)
    op.create_index("ix_port_services_status", "port_services", ["status"], unique=False)
    op.create_index("ix_port_services_updated_at", "port_services", ["updated_at"], unique=False)

    # Create WHOIS records table
    op.create_table(
        "whois_records",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("asn", sa.String(length=32), nullable=True),
        sa.Column("asn_description", sa.String(length=255), nullable=True),
        sa.Column("asn_country_code", sa.String(length=8), nullable=True),
        sa.Column("asn_registry", sa.String(length=255), nullable=True),
        sa.Column("asn_cidr", sa.String(length=64), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("domain_id"),
    )
    op.create_index("ix_whois_records_asn", "whois_records", ["asn"], unique=False)
    op.create_index("ix_whois_records_country_code", "whois_records", ["asn_country_code"], unique=False)
    op.create_index("ix_whois_records_updated_at", "whois_records", ["updated_at"], unique=False)

    # Create geo points table
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

    # Create SSL data table
    op.create_table(
        "ssl_data",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=False),
        sa.Column("issuer_common_name", sa.String(length=255), nullable=True),
        sa.Column("issuer_organizational_unit", sa.String(length=255), nullable=True),
        sa.Column("issuer_organization", sa.String(length=255), nullable=True),
        sa.Column("subject_common_name", sa.String(length=255), nullable=True),
        sa.Column("subject_organizational_unit", sa.String(length=255), nullable=True),
        sa.Column("subject_organization", sa.String(length=255), nullable=True),
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

    # Create SSL subject alt names table
    op.create_table(
        "ssl_subject_alt_names",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("ssl_id", sa.Integer(), nullable=False),
        sa.Column("value", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["ssl_id"], ["ssl_data.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_ssl_subject_alt_names_ssl_id", "ssl_subject_alt_names", ["ssl_id"], unique=False)
    op.create_index("ix_ssl_san_value", "ssl_subject_alt_names", ["value"], unique=False)

    # Create contact messages table
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

    # Create contact rate limits table
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

    # Create request stats table
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

    # Create subnet lookups table
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

    # Create admin users table
    op.create_table(
        "admin_users",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("full_name", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("email", name="ux_admin_users_email"),
    )

    # Create admin tokens table
    op.create_table(
        "admin_tokens",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("token_hash", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["admin_users.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("token_hash", name="ux_admin_tokens_token_hash"),
    )

    # Create URLs table
    op.create_table(
        "urls",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("url", sa.String(length=2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("domain_extracted", sa.DateTime(timezone=True), nullable=True),
        sa.Column("domain_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_urls_url", "urls", ["url"], unique=True)
    op.create_index("ix_urls_created_at", "urls", ["created_at"], unique=False)
    op.create_index("ix_urls_domain_extracted", "urls", ["domain_extracted"], unique=False)

    # Create crawl status table
    op.create_table(
        "crawl_status",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("domain_name", sa.String(length=255), nullable=False),
        sa.Column("domain_id", sa.Integer(), nullable=True),
        sa.Column("domain_crawled", sa.DateTime(timezone=True), nullable=True),
        sa.Column("crawl_failed", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["domain_id"], ["domains.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_crawl_status_domain_name", "crawl_status", ["domain_name"], unique=True)
    op.create_index("ix_crawl_status_domain_crawled", "crawl_status", ["domain_crawled"], unique=False)
    op.create_index("ix_crawl_status_crawl_failed", "crawl_status", ["crawl_failed"], unique=False)


def downgrade() -> None:
    """Drop all tables and indexes."""
    
    # Drop all tables in reverse order (respecting foreign key constraints)
    op.drop_index("ix_crawl_status_crawl_failed", table_name="crawl_status")
    op.drop_index("ix_crawl_status_domain_crawled", table_name="crawl_status")
    op.drop_index("ix_crawl_status_domain_name", table_name="crawl_status")
    op.drop_table("crawl_status")
    
    op.drop_index("ix_urls_domain_extracted", table_name="urls")
    op.drop_index("ix_urls_created_at", table_name="urls")
    op.drop_index("ix_urls_url", table_name="urls")
    op.drop_table("urls")
    
    op.drop_table("admin_tokens")
    op.drop_table("admin_users")
    
    op.drop_index("ix_subnet_lookups_asn", table_name="subnet_lookups")
    op.drop_index("ix_subnet_lookups_cidr", table_name="subnet_lookups")
    op.drop_table("subnet_lookups")
    
    op.drop_index("ix_request_stats_path", table_name="request_stats")
    op.drop_index("ix_request_stats_created", table_name="request_stats")
    op.drop_table("request_stats")
    
    op.drop_index("ix_contact_rate_limits_ip_window", table_name="contact_rate_limits")
    op.drop_table("contact_rate_limits")
    
    op.drop_table("contact_messages")
    
    op.drop_index("ix_ssl_san_value", table_name="ssl_subject_alt_names")
    op.drop_index("ix_ssl_subject_alt_names_ssl_id", table_name="ssl_subject_alt_names")
    op.drop_table("ssl_subject_alt_names")
    
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
    
    op.drop_index("ix_port_services_updated_at", table_name="port_services")
    op.drop_index("ix_port_services_status", table_name="port_services")
    op.drop_index("ix_port_services_port", table_name="port_services")
    op.drop_index("ix_port_services_domain_id", table_name="port_services")
    op.drop_table("port_services")
    
    op.drop_index("ix_txt_records_updated_at", table_name="txt_records")
    op.drop_index("ix_txt_records_domain_id", table_name="txt_records")
    op.drop_table("txt_records")
    
    op.drop_index("ix_cname_records_updated_at", table_name="cname_records")
    op.drop_index("ix_cname_records_target", table_name="cname_records")
    op.drop_index("ix_cname_records_domain_id", table_name="cname_records")
    op.drop_table("cname_records")
    
    op.drop_index("ix_mx_records_updated_at", table_name="mx_records")
    op.drop_index("ix_mx_records_priority", table_name="mx_records")
    op.drop_index("ix_mx_records_exchange", table_name="mx_records")
    op.drop_index("ix_mx_records_domain_id", table_name="mx_records")
    op.drop_table("mx_records")
    
    op.drop_index("ix_soa_records_updated_at", table_name="soa_records")
    op.drop_index("ix_soa_records_value", table_name="soa_records")
    op.drop_index("ix_soa_records_domain_id", table_name="soa_records")
    op.drop_table("soa_records")
    
    op.drop_index("ix_ns_records_updated_at", table_name="ns_records")
    op.drop_index("ix_ns_records_value", table_name="ns_records")
    op.drop_index("ix_ns_records_domain_id", table_name="ns_records")
    op.drop_table("ns_records")
    
    op.drop_index("ix_aaaa_records_updated_at", table_name="aaaa_records")
    op.drop_index("ix_aaaa_records_ip", table_name="aaaa_records")
    op.drop_index("ix_aaaa_records_domain_id", table_name="aaaa_records")
    op.drop_table("aaaa_records")
    
    op.drop_index("ix_a_records_updated_at", table_name="a_records")
    op.drop_index("ix_a_records_ip", table_name="a_records")
    op.drop_index("ix_a_records_domain_id", table_name="a_records")
    op.drop_table("a_records")
    
    op.drop_index("ix_domains_country_code_updated", table_name="domains")
    op.drop_index("ix_domains_state_updated", table_name="domains")
    op.drop_index("ix_domains_country_updated", table_name="domains")
    op.drop_index("ix_domains_city_updated", table_name="domains")
    op.drop_index("ix_domains_name", table_name="domains")
    op.drop_table("domains")