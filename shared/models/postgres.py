"""SQLModel ORM definitions for the PostgreSQL migration."""

from __future__ import annotations

from datetime import datetime, UTC
from typing import Optional, TYPE_CHECKING

from sqlalchemy import (
    Column,
    DateTime,
    Index,
    String,
    Text,
    UniqueConstraint,
    event,
)
from sqlalchemy.orm import relationship
from sqlmodel import Field, SQLModel


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class Domain(SQLModel, table=True):
    __tablename__ = "domains"
    __table_args__ = (
        Index("ix_domains_name", "name", unique=True),
        Index("ix_domains_city_updated", "city", "updated_at"),
        Index("ix_domains_country_updated", "country", "updated_at"),
        Index("ix_domains_state_updated", "state", "updated_at"),
        Index("ix_domains_country_code_updated", "country_code", "updated_at"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=255, nullable=False)
    created_at: datetime = Field(default_factory=utcnow, nullable=False)
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)

    # Common denormalised fields
    country_code: Optional[str] = Field(default=None, max_length=8)
    country: Optional[str] = Field(default=None, max_length=255)
    state: Optional[str] = Field(default=None, max_length=255)
    city: Optional[str] = Field(default=None, max_length=255)
    banner: Optional[str] = Field(default=None)

    # Header shortcuts for quick lookups
    header_status: Optional[str] = Field(default=None, max_length=16)
    header_server: Optional[str] = Field(default=None, max_length=255)
    header_x_powered_by: Optional[str] = Field(default=None, max_length=255)

    # HTTP Response Headers
    header_content_type: Optional[str] = Field(default=None, max_length=255)
    header_content_length: Optional[int] = Field(default=None)
    header_content_encoding: Optional[str] = Field(default=None, max_length=50)
    header_cache_control: Optional[str] = Field(default=None, max_length=255)
    header_etag: Optional[str] = Field(default=None, max_length=255)
    header_set_cookie: Optional[str] = Field(default=None)
    header_location: Optional[str] = Field(default=None, max_length=2048)
    header_www_authenticate: Optional[str] = Field(
        default=None, max_length=255
    )
    header_access_control_allow_origin: Optional[str] = Field(
        default=None, max_length=255
    )
    header_strict_transport_security: Optional[str] = Field(
        default=None, max_length=255
    )
    qrcode: Optional[str] = Field(
        default=None,
        sa_column=Column("qrcode", Text(), nullable=True),
    )
    image: Optional[str] = Field(default=None, max_length=255)
    image_scan_failed: Optional[datetime] = Field(
        default=None,
        sa_column=Column(
            "image_scan_failed", DateTime(timezone=True), nullable=True
        ),
    )


@event.listens_for(Domain, "before_insert", propagate=True)
def _lowercase_domain_before_insert(_mapper, _connection, target: Domain) -> None:
    """Normalise domain names to lowercase before inserting."""
    if target.name:
        target.name = target.name.lower()


@event.listens_for(Domain, "before_update", propagate=True)
def _lowercase_domain_before_update(_mapper, _connection, target: Domain) -> None:
    """Normalise domain names to lowercase before updating."""
    if target.name:
        target.name = target.name.lower()


class ARecord(SQLModel, table=True):
    __tablename__ = "a_records"
    __table_args__ = (
        Index(
            "ux_a_records_domain_id_ip_address",
            "domain_id",
            "ip_address",
            unique=True,
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    domain_id: int = Field(foreign_key="domains.id", nullable=False)
    ip_address: str = Field(
        sa_column=Column("ip_address", String(64), nullable=False)
    )
    created_at: datetime = Field(default_factory=utcnow, nullable=False)
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class AAAARecord(SQLModel, table=True):
    __tablename__ = "aaaa_records"
    __table_args__ = (
        Index(
            "ux_aaaa_records_domain_id_ip_address",
            "domain_id",
            "ip_address",
            unique=True,
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    domain_id: int = Field(foreign_key="domains.id", nullable=False)
    ip_address: str = Field(
        sa_column=Column("ip_address", String(128), nullable=False)
    )
    created_at: datetime = Field(default_factory=utcnow, nullable=False)
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class NSRecord(SQLModel, table=True):
    __tablename__ = "ns_records"
    __table_args__ = (
        Index(
            "ux_ns_records_domain_id_value",
            "domain_id",
            "value",
            unique=True,
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    domain_id: int = Field(foreign_key="domains.id", nullable=False)
    value: str = Field(sa_column=Column("value", String(255), nullable=False))
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class SoaRecord(SQLModel, table=True):
    __tablename__ = "soa_records"
    __table_args__ = (
        Index(
            "ux_soa_records_domain_id_value",
            "domain_id",
            "value",
            unique=True,
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    domain_id: int = Field(foreign_key="domains.id", nullable=False)
    value: str = Field(sa_column=Column("value", String(255), nullable=False))
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class MXRecord(SQLModel, table=True):
    __tablename__ = "mx_records"
    __table_args__ = (
        Index(
            "ux_mx_records_domain_id_exchange_priority",
            "domain_id",
            "exchange",
            "priority",
            unique=True,
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    domain_id: int = Field(foreign_key="domains.id", nullable=False)
    exchange: str = Field(
        sa_column=Column("exchange", String(255), nullable=False)
    )
    priority: Optional[int] = Field(default=None)
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class CNAMERecord(SQLModel, table=True):
    __tablename__ = "cname_records"
    __table_args__ = (
        Index(
            "ux_cname_records_domain_id_target",
            "domain_id",
            "target",
            unique=True,
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    domain_id: int = Field(foreign_key="domains.id", nullable=False)
    target: str = Field(
        sa_column=Column("target", String(255), nullable=False)
    )
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class TXTRecord(SQLModel, table=True):
    __tablename__ = "txt_records"
    __table_args__ = (
        Index(
            "ux_txt_records_domain_id_content",
            "domain_id",
            "content",
            unique=True,
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    domain_id: int = Field(foreign_key="domains.id", nullable=False)
    content: str = Field(
        sa_column=Column("content", String(1000), nullable=False)
    )
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class PortService(SQLModel, table=True):
    __tablename__ = "port_services"
    __table_args__ = (
        UniqueConstraint(
            "domain_id", "port", name="uq_port_services_domain_id_port"
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    domain_id: int = Field(foreign_key="domains.id", nullable=False)
    port: int = Field(nullable=False)
    status: Optional[str] = Field(default=None, max_length=32)
    service: Optional[str] = Field(default=None, max_length=255)
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class WhoisRecord(SQLModel, table=True):
    __tablename__ = "whois_records"

    id: Optional[int] = Field(default=None, primary_key=True)
    # One-to-one relationship with Domain
    domain_id: int = Field(
        foreign_key="domains.id", nullable=False, unique=True
    )
    asn: Optional[str] = Field(default=None, max_length=32)
    asn_description: Optional[str] = Field(default=None, max_length=255)
    asn_country_code: Optional[str] = Field(default=None, max_length=8)
    asn_registry: Optional[str] = Field(default=None, max_length=255)
    asn_cidr: Optional[str] = Field(default=None, max_length=64)
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class GeoPoint(SQLModel, table=True):
    __tablename__ = "geo_points"

    id: Optional[int] = Field(default=None, primary_key=True)
    # One-to-one relationship with Domain
    domain_id: int = Field(
        foreign_key="domains.id", nullable=False, unique=True
    )
    latitude: Optional[float] = Field(default=None)
    longitude: Optional[float] = Field(default=None)
    country_code: Optional[str] = Field(default=None, max_length=8)
    country: Optional[str] = Field(default=None, max_length=255)
    state: Optional[str] = Field(default=None, max_length=255)
    city: Optional[str] = Field(default=None, max_length=255)
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class SSLData(SQLModel, table=True):
    __tablename__ = "ssl_data"

    id: Optional[int] = Field(default=None, primary_key=True)
    # One-to-one relationship with Domain
    domain_id: int = Field(
        foreign_key="domains.id", nullable=False, unique=True
    )
    issuer_common_name: Optional[str] = Field(default=None, max_length=255)
    issuer_organizational_unit: Optional[str] = Field(
        default=None, max_length=255
    )
    issuer_organization: Optional[str] = Field(default=None, max_length=255)
    subject_common_name: Optional[str] = Field(default=None, max_length=255)
    subject_organizational_unit: Optional[str] = Field(
        default=None, max_length=255
    )
    subject_organization: Optional[str] = Field(default=None, max_length=255)
    serial_number: Optional[str] = Field(default=None, max_length=255)
    ocsp: Optional[str] = Field(default=None)
    ca_issuers: Optional[str] = Field(default=None)
    crl_distribution_points: Optional[str] = Field(default=None)
    not_before: Optional[datetime] = Field(default=None)
    not_after: Optional[datetime] = Field(default=None)
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class SSLSubjectAltName(SQLModel, table=True):
    __tablename__ = "ssl_subject_alt_names"

    id: Optional[int] = Field(default=None, primary_key=True)
    ssl_id: int = Field(foreign_key="ssl_data.id", nullable=False)
    value: str = Field(sa_column=Column("value", String(255), nullable=False))


class ContactMessage(SQLModel, table=True):
    __tablename__ = "contact_messages"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: Optional[str] = Field(default=None, max_length=100)
    email: Optional[str] = Field(default=None, max_length=255)
    subject: Optional[str] = Field(default=None, max_length=255)
    message: Optional[str] = Field(default=None)
    ip_address: Optional[str] = Field(default=None, max_length=64)
    created_at: datetime = Field(default_factory=utcnow, nullable=False)


class ContactRateLimit(SQLModel, table=True):
    __tablename__ = "contact_rate_limits"
    __table_args__ = (
        Index(
            "ix_contact_rate_limits_ip_window",
            "ip_address",
            "window_start",
            unique=True,
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    ip_address: str = Field(max_length=64, nullable=False)
    window_start: datetime = Field(nullable=False)
    expires_at: datetime = Field(nullable=False)
    count: int = Field(default=0, nullable=False)


class RequestStat(SQLModel, table=True):
    __tablename__ = "request_stats"
    __table_args__ = (
        Index("ix_request_stats_created", "created_at"),
        Index("ix_request_stats_path", "path"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=utcnow, nullable=False)
    path: Optional[str] = Field(default=None, max_length=512)
    query: Optional[str] = Field(default=None, max_length=512)
    fragment: Optional[str] = Field(default=None, max_length=255)
    request_method: Optional[str] = Field(default=None, max_length=16)
    status_code: Optional[int] = Field(default=None)
    remote_address: Optional[str] = Field(default=None, max_length=64)
    scheme: Optional[str] = Field(default=None, max_length=16)
    host: Optional[str] = Field(default=None, max_length=255)
    port: Optional[str] = Field(default=None, max_length=16)
    origin: Optional[str] = Field(default=None, max_length=255)
    accept: Optional[str] = Field(default=None, max_length=255)
    referer: Optional[str] = Field(default=None, max_length=255)
    connection: Optional[str] = Field(default=None, max_length=64)
    user_agent: Optional[str] = Field(default=None)
    accept_language: Optional[str] = Field(default=None, max_length=255)
    accept_encoding: Optional[str] = Field(default=None, max_length=255)


class Url(SQLModel, table=True):
    __tablename__ = "urls"
    __table_args__ = (
        Index("ix_urls_url", "url", unique=True),
        Index("ix_urls_created_at", "created_at"),
        Index("ix_urls_domain_extracted", "domain_extracted"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    url: str = Field(max_length=2048, nullable=False)  # URLs can be long
    created_at: datetime = Field(default_factory=utcnow, nullable=False)
    domain_extracted: Optional[datetime] = Field(default=None)
    domain_id: Optional[int] = Field(default=None, foreign_key="domains.id")


class CrawlStatus(SQLModel, table=True):
    __tablename__ = "crawl_status"
    __table_args__ = (
        Index("ix_crawl_status_domain_name", "domain_name", unique=True),
        Index("ix_crawl_status_domain_crawled", "domain_crawled"),
        Index("ix_crawl_status_crawl_failed", "crawl_failed"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    domain_name: str = Field(max_length=255, nullable=False)
    domain_id: Optional[int] = Field(default=None, foreign_key="domains.id")
    domain_crawled: Optional[datetime] = Field(default=None)
    crawl_failed: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=utcnow, nullable=False)
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class SubnetLookup(SQLModel, table=True):
    __tablename__ = "subnet_lookups"
    __table_args__ = (
        Index("ix_subnet_lookups_cidr", "cidr"),
        Index("ix_subnet_lookups_asn", "asn"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    domain_id: Optional[int] = Field(default=None, foreign_key="domains.id")
    cidr: str = Field(max_length=64, nullable=False)
    ip_start: Optional[str] = Field(default=None, max_length=64)
    ip_end: Optional[str] = Field(default=None, max_length=64)
    asn: Optional[str] = Field(default=None, max_length=32)
    asn_description: Optional[str] = Field(default=None)
    source: Optional[str] = Field(default=None, max_length=64)
    created_at: datetime = Field(default_factory=utcnow, nullable=False)
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class AdminUser(SQLModel, table=True):
    __tablename__ = "admin_users"
    __table_args__ = (
        UniqueConstraint("email", name="ux_admin_users_email"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(sa_column=Column("email", String(255), nullable=False))
    password_hash: str = Field(sa_column=Column(
        "password_hash", String(255), nullable=False))
    full_name: Optional[str] = Field(default=None, max_length=255)
    created_at: datetime = Field(default_factory=utcnow, nullable=False)
    updated_at: datetime = Field(default_factory=utcnow, nullable=False)


class AdminToken(SQLModel, table=True):
    __tablename__ = "admin_tokens"
    __table_args__ = (
        UniqueConstraint("token_hash", name="ux_admin_tokens_token_hash"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="admin_users.id", nullable=False)
    token_hash: str = Field(sa_column=Column(
        "token_hash", String(128), nullable=False))
    created_at: datetime = Field(default_factory=utcnow, nullable=False)
    expires_at: datetime = Field(default_factory=utcnow, nullable=False)


if TYPE_CHECKING:  # pragma: no cover - typing helpers
    Domain.a_records: list[ARecord]
    Domain.aaaa_records: list[AAAARecord]
    Domain.ns_records: list[NSRecord]
    Domain.soa_records: list[SoaRecord]
    Domain.mx_records: list[MXRecord]
    Domain.cname_records: list[CNAMERecord]
    Domain.txt_records: list[TXTRecord]
    Domain.ports: list[PortService]
    Domain.whois: Optional[WhoisRecord]
    Domain.geo: Optional[GeoPoint]
    Domain.ssl: Optional[SSLData]
    Domain.urls: list[Url]
    Domain.crawl_status: Optional[CrawlStatus]

    ARecord.domain: Domain
    AAAARecord.domain: Domain
    NSRecord.domain: Domain
    SoaRecord.domain: Domain
    MXRecord.domain: Domain
    CNAMERecord.domain: Domain
    TXTRecord.domain: Domain
    PortService.domain: Domain
    WhoisRecord.domain: Domain
    GeoPoint.domain: Domain
    SSLData.domain: Domain
    SSLData.subject_alt_names: list[SSLSubjectAltName]
    SSLSubjectAltName.ssl: SSLData
    SubnetLookup.domain: Optional[Domain]
    AdminToken.user: AdminUser
    AdminUser.tokens: list[AdminToken]


# Assign relationships after all classes are defined to avoid circular imports
Domain.a_records = relationship(ARecord, back_populates="domain")
Domain.aaaa_records = relationship(AAAARecord, back_populates="domain")
Domain.ns_records = relationship(NSRecord, back_populates="domain")
Domain.soa_records = relationship(SoaRecord, back_populates="domain")
Domain.mx_records = relationship(MXRecord, back_populates="domain")
Domain.cname_records = relationship(CNAMERecord, back_populates="domain")
Domain.txt_records = relationship(TXTRecord, back_populates="domain")
Domain.ports = relationship(PortService, back_populates="domain")
Domain.whois = relationship(
    WhoisRecord, back_populates="domain", uselist=False
)
Domain.geo = relationship(GeoPoint, back_populates="domain", uselist=False)
Domain.ssl = relationship(SSLData, back_populates="domain", uselist=False)
Domain.subnet_lookups = relationship(SubnetLookup, back_populates="domain")
Domain.urls = relationship(Url, back_populates="domain")
Domain.crawl_status = relationship(
    CrawlStatus, back_populates="domain", uselist=False
)

ARecord.domain = relationship(Domain, back_populates="a_records")
AAAARecord.domain = relationship(Domain, back_populates="aaaa_records")
NSRecord.domain = relationship(Domain, back_populates="ns_records")
SoaRecord.domain = relationship(Domain, back_populates="soa_records")
MXRecord.domain = relationship(Domain, back_populates="mx_records")
CNAMERecord.domain = relationship(Domain, back_populates="cname_records")
TXTRecord.domain = relationship(Domain, back_populates="txt_records")
PortService.domain = relationship(Domain, back_populates="ports")
WhoisRecord.domain = relationship(Domain, back_populates="whois")
GeoPoint.domain = relationship(Domain, back_populates="geo")
SSLData.domain = relationship(Domain, back_populates="ssl")
SSLData.subject_alt_names = relationship(
    SSLSubjectAltName, back_populates="ssl"
)
SSLSubjectAltName.ssl = relationship(
    SSLData, back_populates="subject_alt_names"
)
SubnetLookup.domain = relationship(Domain, back_populates="subnet_lookups")
AdminUser.tokens = relationship(
    AdminToken,
    back_populates="user",
    cascade="all, delete-orphan",
)
AdminToken.user = relationship(AdminUser, back_populates="tokens")

Url.domain = relationship("Domain", back_populates="urls")
CrawlStatus.domain = relationship("Domain", back_populates="crawl_status")

__all__ = [
    "utcnow",
    "Domain",
    "ARecord",
    "AAAARecord",
    "NSRecord",
    "SoaRecord",
    "MXRecord",
    "CNAMERecord",
    "TXTRecord",
    "PortService",
    "WhoisRecord",
    "GeoPoint",
    "SSLData",
    "SSLSubjectAltName",
    "ContactMessage",
    "ContactRateLimit",
    "RequestStat",
    "Url",
    "CrawlStatus",
    "SubnetLookup",
    "AdminUser",
    "AdminToken",
]
