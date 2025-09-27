"""Add performance indexes for query optimization.

This migration adds comprehensive indexes to optimize the most common query
patterns used in the application, including:
- Text search indexes for banner, header fields, SSL data
- Composite indexes for common filtering and sorting patterns
- Geospatial indexes for location-based queries
- Full-text search capabilities

Revision ID: 202501042200
Revises: 202501031000
Create Date: 2025-01-04 22:00:00.000000

"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "202501042200"
down_revision = "202501031000"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add comprehensive performance indexes."""

    # Enable required PostgreSQL extensions
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # ===== DOMAIN TABLE INDEXES =====
    # Text search for banner field
    op.create_index(
        "ix_domains_banner_gin",
        "domains",
        ["banner"],
        postgresql_using="gin",
        postgresql_ops={"banner": "gin_trgm_ops"}
    )

    # Composite indexes for common filtering patterns
    op.create_index(
        "ix_domains_country_updated",
        "domains",
        ["country_code", "updated_at"]
    )
    op.create_index(
        "ix_domains_state_updated",
        "domains",
        ["state", "updated_at"]
    )
    op.create_index(
        "ix_domains_city_updated",
        "domains",
        ["city", "updated_at"]
    )

    # ===== A_RECORDS TABLE INDEXES =====
    # Composite index for IP+domain lookups
    op.create_index(
        "ix_a_records_ip_domain",
        "a_records",
        ["ip_address", "domain_id"]
    )
    op.create_index(
        "ix_a_records_domain_updated",
        "a_records",
        ["domain_id", "updated_at"]
    )

    # ===== AAAA_RECORDS TABLE INDEXES =====
    op.create_index(
        "ix_aaaa_records_ip_domain",
        "aaaa_records",
        ["ip_address", "domain_id"]
    )
    op.create_index(
        "ix_aaaa_records_domain_updated",
        "aaaa_records",
        ["domain_id", "updated_at"]
    )

    # ===== DNS RECORD TABLE INDEXES =====
    # MX Records
    op.create_index(
        "ix_mx_records_exchange_gin",
        "mx_records",
        ["exchange"],
        postgresql_using="gin",
        postgresql_ops={"exchange": "gin_trgm_ops"}
    )
    op.create_index(
        "ix_mx_records_domain_updated",
        "mx_records",
        ["domain_id", "updated_at"]
    )

    # NS Records
    op.create_index(
        "ix_ns_records_value_gin",
        "ns_records",
        ["value"],
        postgresql_using="gin",
        postgresql_ops={"value": "gin_trgm_ops"}
    )
    op.create_index(
        "ix_ns_records_domain_updated",
        "ns_records",
        ["domain_id", "updated_at"]
    )

    # ===== GEOSPATIAL INDEXES =====
    # For geo_points table location queries
    op.create_index(
        "ix_geo_points_location",
        "geo_points",
        ["latitude", "longitude"]
    )
    op.create_index(
        "ix_geo_points_domain_updated",
        "geo_points",
        ["domain_id", "updated_at"]
    )

    # ===== WHOIS RECORD INDEXES =====
    # Text search for ASN description
    op.create_index(
        "ix_whois_records_asn_desc_gin",
        "whois_records",
        ["asn_description"],
        postgresql_using="gin",
        postgresql_ops={"asn_description": "gin_trgm_ops"}
    )

    # Composite indexes for WHOIS filtering
    op.create_index(
        "ix_whois_records_country_updated",
        "whois_records",
        ["asn_country_code", "updated_at"]
    )
    op.create_index(
        "ix_whois_records_asn_registry",
        "whois_records",
        ["asn", "asn_registry"]
    )
    op.create_index(
        "ix_whois_records_asn_cidr",
        "whois_records",
        ["asn", "asn_cidr"]
    )
    op.create_index(
        "ix_whois_records_asn_updated",
        "whois_records",
        ["asn", "updated_at"]
    )

    # ===== PORT SERVICE INDEXES =====
    op.create_index(
        "ix_port_services_domain_updated",
        "port_services",
        ["domain_id", "updated_at"]
    )
    op.create_index(
        "ix_port_services_service_gin",
        "port_services",
        ["service"],
        postgresql_using="gin",
        postgresql_ops={"service": "gin_trgm_ops"}
    )
    op.create_index(
        "ix_port_services_port_domain",
        "port_services",
        ["port", "domain_id"]
    )

    # ===== CNAME RECORD INDEXES =====
    op.create_index(
        "ix_cname_records_domain_updated",
        "cname_records",
        ["domain_id", "updated_at"]
    )
    op.create_index(
        "ix_cname_records_target_gin",
        "cname_records",
        ["target"],
        postgresql_using="gin",
        postgresql_ops={"target": "gin_trgm_ops"}
    )


def downgrade() -> None:
    """Remove performance indexes."""

    # CNAME record indexes
    op.drop_index("ix_cname_records_target_gin", table_name="cname_records")
    op.drop_index(
        "ix_cname_records_domain_updated", table_name="cname_records"
    )

    # Port services indexes
    op.drop_index("ix_port_services_port_domain", table_name="port_services")
    op.drop_index("ix_port_services_service_gin", table_name="port_services")
    op.drop_index(
        "ix_port_services_domain_updated", table_name="port_services"
    )

    # WHOIS records indexes
    op.drop_index("ix_whois_records_asn_updated", table_name="whois_records")
    op.drop_index("ix_whois_records_asn_cidr", table_name="whois_records")
    op.drop_index("ix_whois_records_asn_registry", table_name="whois_records")
    op.drop_index(
        "ix_whois_records_country_updated", table_name="whois_records"
    )
    op.drop_index("ix_whois_records_asn_desc_gin", table_name="whois_records")

    # Geospatial indexes
    op.drop_index(
        "ix_geo_points_domain_updated", table_name="geo_points"
    )
    op.drop_index("ix_geo_points_location", table_name="geo_points")

    # NS record indexes
    op.drop_index("ix_ns_records_domain_updated", table_name="ns_records")
    op.drop_index("ix_ns_records_value_gin", table_name="ns_records")

    # MX record indexes
    op.drop_index("ix_mx_records_domain_updated", table_name="mx_records")
    op.drop_index("ix_mx_records_exchange_gin", table_name="mx_records")

    # AAAA record indexes
    op.drop_index(
        "ix_aaaa_records_domain_updated", table_name="aaaa_records"
    )
    op.drop_index("ix_aaaa_records_ip_domain", table_name="aaaa_records")

    # A record indexes
    op.drop_index("ix_a_records_domain_updated", table_name="a_records")
    op.drop_index("ix_a_records_ip_domain", table_name="a_records")

    # Domain indexes
    op.drop_index("ix_domains_city_updated", table_name="domains")
    op.drop_index("ix_domains_state_updated", table_name="domains")
    op.drop_index("ix_domains_country_updated", table_name="domains")
    op.drop_index("ix_domains_banner_gin", table_name="domains")
