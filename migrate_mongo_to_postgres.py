#!/usr/bin/env python3
"""
MongoDB to PostgreSQL Migration Script

This script migrates data from MongoDB collections to PostgreSQL tables
using the existing SQLModel schema. It handles the main collections:
- ip_data.dns -> domains and related tables
- url_data.url -> (new url_data table)
- ip_data.asn -> (embedded in whois records)

Usage:
    python migrate_mongo_to_postgres.py --mongo-uri mongodb://localhost:27017 --batch-size 1000
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import click
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.db_postgres import get_session_factory
from app.models.postgres import (
    AAAARecord,
    ARecord,
    CNAMERecord,
    Domain,
    GeoPoint,
    MXRecord,
    NSRecord,
    PortService,
    SoaRecord,
    SSLData,
    SSLSubjectAltName,
    WhoisRecord,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


metadata = MetaData()
url_data_table = Table(
    "url_data",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("url", Text, nullable=False, unique=True),
    Column("domain", Text, nullable=True),
    Column(
        "created_at",
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now(),
    ),
    Column("domain_extracted", DateTime(timezone=False)),
    Column("crawl_failed", DateTime(timezone=False)),
    Column("domain_crawled", DateTime(timezone=False)),
)

Index("ix_url_data_domain", url_data_table.c.domain)
Index("ix_url_data_created_at", url_data_table.c.created_at)


class MigrationStats:
    def __init__(self):
        self.domains_processed = 0
        self.domains_created = 0
        self.domains_updated = 0
        self.records_created = 0
        self.errors = 0

    def log_progress(self):
        logger.info(
            f"Progress: {self.domains_processed} processed, "
            f"{self.domains_created} created, {self.domains_updated} updated, "
            f"{self.records_created} records, {self.errors} errors"
        )


def parse_datetime(value: Any) -> Optional[datetime]:
    """Parse MongoDB datetime fields safely."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, str):
        try:
            # Handle ISO format with Z
            if value.endswith('Z'):
                value = value[:-1] + '+00:00'
            dt = datetime.fromisoformat(value)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except ValueError:
            return None
    return None


def extract_geo_data(mongo_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Extract geographic data from MongoDB document."""
    geo = mongo_doc.get('geo', {})
    if not geo:
        return {}
    
    return {
        'country_code': geo.get('country_code'),
        'country': geo.get('country'),
        'state': geo.get('state'),
        'city': geo.get('city'),
        'latitude': geo.get('latitude'),
        'longitude': geo.get('longitude'),
    }


def extract_header_data(mongo_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Extract header data from MongoDB document."""
    header = mongo_doc.get('header', {})
    if not header:
        return {}
    
    return {
        'header_status': (
            str(header.get('status', ''))[:16] 
            if header.get('status') else None
        ),
        'header_server': (
            str(header.get('server', ''))[:255] 
            if header.get('server') else None
        ),
        'header_x_powered_by': (
            str(header.get('x-powered-by', ''))[:255]
            if header.get('x-powered-by') else None
        ),
    }


async def migrate_domain_document(
    session: AsyncSession,
    mongo_doc: Dict[str, Any],
    stats: MigrationStats
) -> Optional[Domain]:
    """Migrate a single MongoDB dns document to PostgreSQL."""
    try:
        domain_name = mongo_doc.get('domain', '').lower().strip()
        if not domain_name:
            logger.warning(f"Skipping document with missing domain: {mongo_doc.get('_id')}")
            stats.errors += 1
            return None

        # Check if domain already exists
        stmt = select(Domain).where(Domain.name == domain_name)
        result = await session.exec(stmt)
        existing_domain = result.first()

        # Extract timestamps
        created_at = parse_datetime(mongo_doc.get('created')) or datetime.utcnow()
        updated_at = parse_datetime(mongo_doc.get('updated')) or created_at

        # Extract geographic and header data
        geo_data = extract_geo_data(mongo_doc)
        header_data = extract_header_data(mongo_doc)

        if existing_domain:
            # Update existing domain
            for key, value in geo_data.items():
                if value is not None:
                    setattr(existing_domain, key, value)
            
            for key, value in header_data.items():
                if value is not None:
                    setattr(existing_domain, key, value)

            if mongo_doc.get('banner'):
                existing_domain.banner = mongo_doc['banner']

            existing_domain.updated_at = updated_at
            session.add(existing_domain)
            stats.domains_updated += 1
            domain = existing_domain
        else:
            # Create new domain
            domain_data = {
                'name': domain_name,
                'created_at': created_at,
                'updated_at': updated_at,
                'banner': mongo_doc.get('banner'),
                **geo_data,
                **header_data
            }
            domain = Domain(**domain_data)
            session.add(domain)
            await session.flush()  # Get the ID
            stats.domains_created += 1

        # Migrate DNS records
        await migrate_dns_records(session, domain, mongo_doc, stats)

        # Migrate SSL data
        await migrate_ssl_data(session, domain, mongo_doc, stats)

        # Migrate WHOIS data
        await migrate_whois_data(session, domain, mongo_doc, stats)

        # Migrate port/service data
        await migrate_port_data(session, domain, mongo_doc, stats)

        # Migrate geo point if coordinates exist
        if geo_data.get('latitude') and geo_data.get('longitude'):
            await migrate_geo_point(session, domain, geo_data, stats)

        stats.domains_processed += 1
        return domain

    except Exception as e:
        logger.error(f"Error migrating domain {mongo_doc.get('domain', 'unknown')}: {e}")
        stats.errors += 1
        return None


async def migrate_dns_records(
    session: AsyncSession,
    domain: Domain,
    mongo_doc: Dict[str, Any],
    stats: MigrationStats
):
    """Migrate DNS records from MongoDB document."""
    updated_at = parse_datetime(mongo_doc.get('updated')) or datetime.utcnow()

    # A Records
    a_records = mongo_doc.get('a_record', [])
    if isinstance(a_records, list):
        for ip in a_records:
            if ip and isinstance(ip, str):
                record = ARecord(
                    domain_id=domain.id,
                    ip_address=ip,
                    updated_at=updated_at
                )
                session.add(record)
                stats.records_created += 1

    # AAAA Records
    aaaa_records = mongo_doc.get('aaaa_record', [])
    if isinstance(aaaa_records, list):
        for ip in aaaa_records:
            if ip and isinstance(ip, str):
                record = AAAARecord(
                    domain_id=domain.id,
                    ip_address=ip,
                    updated_at=updated_at
                )
                session.add(record)
                stats.records_created += 1

    # NS Records
    ns_records = mongo_doc.get('ns_record', [])
    if isinstance(ns_records, list):
        for ns in ns_records:
            if ns and isinstance(ns, str):
                record = NSRecord(
                    domain_id=domain.id,
                    value=ns[:255],
                    updated_at=updated_at
                )
                session.add(record)
                stats.records_created += 1

    # SOA Records
    soa_records = mongo_doc.get('soa_record', [])
    if isinstance(soa_records, list):
        for soa in soa_records:
            if soa and isinstance(soa, str):
                record = SoaRecord(
                    domain_id=domain.id,
                    value=soa[:255],
                    updated_at=updated_at
                )
                session.add(record)
                stats.records_created += 1

    # MX Records
    mx_records = mongo_doc.get('mx_record', [])
    if isinstance(mx_records, list):
        for mx in mx_records:
            if isinstance(mx, dict) and mx.get('exchange'):
                record = MXRecord(
                    domain_id=domain.id,
                    exchange=mx['exchange'][:255],
                    priority=mx.get('priority'),
                    updated_at=updated_at
                )
                session.add(record)
                stats.records_created += 1

    # CNAME Records
    cname_records = mongo_doc.get('cname_record', [])
    if isinstance(cname_records, list):
        for cname in cname_records:
            if isinstance(cname, dict) and cname.get('target'):
                record = CNAMERecord(
                    domain_id=domain.id,
                    target=cname['target'][:255],
                    updated_at=updated_at
                )
                session.add(record)
                stats.records_created += 1

    # TXT Records
    txt_records = mongo_doc.get('txt_record', [])
    if isinstance(txt_records, list):
        for txt in txt_records:
            if txt and isinstance(txt, str):
                # If you have a TxtRecord model, use it here
                from app.models.postgres import TxtRecord
                record = TxtRecord(
                    domain_id=domain.id,
                    value=txt[:255],
                    updated_at=updated_at
                )
                session.add(record)
                stats.records_created += 1


async def migrate_ssl_data(
    session: AsyncSession,
    domain: Domain,
    mongo_doc: Dict[str, Any],
    stats: MigrationStats
):
    """Migrate SSL certificate data."""
    ssl_data = mongo_doc.get('ssl', {})
    if not ssl_data or not isinstance(ssl_data, dict):
        return

    updated_at = parse_datetime(mongo_doc.get('updated')) or datetime.utcnow()

    issuer = ssl_data.get('issuer', {})
    subject = ssl_data.get('subject', {})

    ssl_record = SSLData(
        domain_id=domain.id,
        serial_number=ssl_data.get('serial_number'),
        issuer_common_name=issuer.get('common_name'),
        issuer_organizational_unit=issuer.get('organizational_unit_name'),
        issuer_organization=issuer.get('organization_name'),
        subject_common_name=subject.get('common_name'),
        subject_organizational_unit=subject.get('organizational_unit_name'),
        subject_organization=subject.get('organization_name'),
        not_before=parse_datetime(ssl_data.get('not_before')),
        not_after=parse_datetime(ssl_data.get('not_after')),
        updated_at=updated_at
    )
    session.add(ssl_record)
    await session.flush()

    # Subject Alternative Names
    san_list = ssl_data.get('subject_alt_names', [])
    if isinstance(san_list, list):
        for san in san_list:
            if san and isinstance(san, str):
                san_record = SSLSubjectAltName(
                    ssl_id=ssl_record.id,
                    value=san[:255]
                )
                session.add(san_record)

    stats.records_created += 1


async def migrate_whois_data(
    session: AsyncSession,
    domain: Domain,
    mongo_doc: Dict[str, Any],
    stats: MigrationStats
):
    """Migrate WHOIS data."""
    whois_data = mongo_doc.get('whois', {})
    if not whois_data or not isinstance(whois_data, dict):
        return

    updated_at = parse_datetime(mongo_doc.get('updated')) or datetime.utcnow()

    whois_record = WhoisRecord(
        domain_id=domain.id,
        asn=whois_data.get('asn'),
        asn_description=whois_data.get('asn_description'),
        asn_country_code=whois_data.get('asn_country_code'),
        asn_registry=whois_data.get('asn_registry'),
        asn_cidr=whois_data.get('asn_cidr'),
        updated_at=updated_at
    )
    session.add(whois_record)
    stats.records_created += 1


async def migrate_port_data(
    session: AsyncSession,
    domain: Domain,
    mongo_doc: Dict[str, Any],
    stats: MigrationStats
):
    """Migrate port/service data."""
    ports_data = mongo_doc.get('ports', [])
    if not isinstance(ports_data, list):
        return

    updated_at = parse_datetime(mongo_doc.get('updated')) or datetime.utcnow()

    for port_info in ports_data:
        if isinstance(port_info, dict) and port_info.get('port'):
            port_record = PortService(
                domain_id=domain.id,
                port=port_info['port'],
                service=port_info.get('service', port_info.get('protocol')),  # Handle legacy field name
                banner=port_info.get('banner'),
                updated_at=updated_at
            )
            session.add(port_record)
            stats.records_created += 1


async def migrate_geo_point(
    session: AsyncSession,
    domain: Domain,
    geo_data: Dict[str, Any],
    stats: MigrationStats
):
    """Migrate geographic point data."""
    lat = geo_data.get('latitude')
    lon = geo_data.get('longitude')
    
    if lat is not None and lon is not None:
        try:
            geo_point = GeoPoint(
                domain_id=domain.id,
                latitude=float(lat),
                longitude=float(lon),
                country_code=geo_data.get('country_code'),
                country=geo_data.get('country'),
                state=geo_data.get('state'),
                city=geo_data.get('city'),
                updated_at=datetime.utcnow()
            )
            session.add(geo_point)
            stats.records_created += 1
        except (ValueError, TypeError):
            logger.warning(f"Invalid coordinates for domain {domain.name}: lat={lat}, lon={lon}")


async def migrate_dns_collection(
    mongo_uri: str,
    batch_size: int,
    dry_run: bool = False
) -> MigrationStats:
    """Migrate the MongoDB dns collection to PostgreSQL."""
    stats = MigrationStats()
    
    # Connect to MongoDB
    mongo_client = AsyncIOMotorClient(mongo_uri)
    mongo_db = mongo_client.ip_data
    
    # Connect to PostgreSQL
    session_factory = get_session_factory()
    
    try:
        # Get total count
        total_docs = await mongo_db.dns.count_documents({})
        logger.info(f"Total documents to migrate: {total_docs}")
        
        # Process in batches
        skip = 0
        while skip < total_docs:
            async with session_factory() as session:
                # Get batch from MongoDB
                cursor = mongo_db.dns.find({}).skip(skip).limit(batch_size)
                batch = await cursor.to_list(length=batch_size)
                
                if not batch:
                    break
                
                logger.info(f"Processing batch {skip}-{skip + len(batch)}")
                
                # Process each document in the batch
                for mongo_doc in batch:
                    await migrate_domain_document(session, mongo_doc, stats)
                
                if not dry_run:
                    try:
                        await session.commit()
                    except Exception as e:
                        logger.error(f"Error committing batch: {e}")
                        await session.rollback()
                        stats.errors += len(batch)
                
                stats.log_progress()
                skip += len(batch)
    
    finally:
        mongo_client.close()
    
    return stats


async def create_url_table(session_factory):
    """Create URL table for migrating url_data collection."""
    engine = getattr(session_factory, "bind", None)
    if engine is None:
        engine = session_factory.kw.get("bind")  # type: ignore[attr-defined]
    if engine is None:
        raise RuntimeError("Session factory is not bound to an engine")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: metadata.create_all(
                sync_conn, tables=[url_data_table], checkfirst=True
            )
        )


async def migrate_url_collection(
    mongo_uri: str,
    batch_size: int,
    dry_run: bool = False
) -> int:
    """Migrate the MongoDB url collection to PostgreSQL."""
    migrated_count = 0
    
    # Connect to MongoDB
    mongo_client = AsyncIOMotorClient(mongo_uri)
    mongo_db = mongo_client.url_data
    
    # Connect to PostgreSQL
    session_factory = get_session_factory()
    
    # Create URL table
    await create_url_table(session_factory)
    
    try:
        # Get total count
        total_docs = await mongo_db.url.count_documents({})
        logger.info(f"Total URL documents to migrate: {total_docs}")
        
        # Process in batches
        skip = 0
        while skip < total_docs:
            async with session_factory() as session:
                # Get batch from MongoDB
                cursor = mongo_db.url.find({}).skip(skip).limit(batch_size)
                batch = await cursor.to_list(length=batch_size)
                
                if not batch:
                    break
                
                logger.info(f"Processing URL batch {skip}-{skip + len(batch)}")
                
                # Prepare batch insert
                url_records = []
                for doc in batch:
                    url = doc.get('url')
                    if not url:
                        continue
                    
                    # Extract domain from URL
                    domain = None
                    try:
                        parsed = urlparse(url)
                        domain = parsed.netloc.lower()
                    except Exception:
                        pass
                    
                    url_records.append({
                        'url': url,
                        'domain': domain,
                        'created_at': parse_datetime(doc.get('created')) or datetime.utcnow(),
                        'domain_extracted': parse_datetime(doc.get('domain_extracted')),
                        'crawl_failed': parse_datetime(doc.get('crawl_failed')),
                        'domain_crawled': parse_datetime(doc.get('domain_crawled')),
                    })
                
                if url_records and not dry_run:
                    try:
                        # Bulk insert with PostgreSQL upsert semantics to avoid duplicates
                        stmt = (
                            insert(url_data_table)
                            .values(url_records)
                            .on_conflict_do_nothing(index_elements=[url_data_table.c.url])
                        )
                        await session.exec(stmt)
                        await session.commit()
                        migrated_count += len(url_records)
                    except Exception as e:
                        logger.error(f"Error inserting URL batch: {e}")
                        await session.rollback()
                
                skip += len(batch)
    
    finally:
        mongo_client.close()
    
    logger.info(f"Migrated {migrated_count} URL records")
    return migrated_count


@click.command()
@click.option('--mongo-uri', default='mongodb://localhost:27017', help='MongoDB connection URI')
@click.option('--batch-size', default=1000, help='Batch size for processing')
@click.option('--dry-run', is_flag=True, help='Run without making changes')
@click.option('--skip-dns', is_flag=True, help='Skip DNS collection migration')
@click.option('--skip-urls', is_flag=True, help='Skip URL collection migration')
def main(mongo_uri: str, batch_size: int, dry_run: bool, skip_dns: bool, skip_urls: bool):
    """Migrate data from MongoDB to PostgreSQL."""
    
    async def run_migration():
        logger.info("Starting MongoDB to PostgreSQL migration")
        
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
        
        # Migrate DNS collection
        if not skip_dns:
            logger.info("Migrating DNS collection...")
            dns_stats = await migrate_dns_collection(mongo_uri, batch_size, dry_run)
            logger.info(f"DNS migration completed: {dns_stats.domains_processed} domains processed")
        
        # Migrate URL collection
        if not skip_urls:
            logger.info("Migrating URL collection...")
            url_count = await migrate_url_collection(mongo_uri, batch_size, dry_run)
            logger.info(f"URL migration completed: {url_count} URLs migrated")
        
        logger.info("Migration completed successfully!")
    
    asyncio.run(run_migration())


# NOTE: When querying Domain objects in FastAPI, always use selectinload for relationships
# to avoid MissingGreenlet errors due to async lazy loading.
# Example:
# stmt = select(Domain).options(selectinload(Domain.txt_records))


if __name__ == '__main__':
    main()
