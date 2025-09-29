"""
Shared DNS utilities and components.

Common components used by both the DNS worker service and publisher service.
"""

from __future__ import annotations

from importlib import import_module

try:
    import bootstrap  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for module execution
    bootstrap = import_module("tools.bootstrap")

bootstrap.setup()

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import AsyncIterator, Dict, List, Optional, Set, Tuple

from dns import resolver
from dns.exception import DNSException, Timeout
from dns.name import EmptyLabel, LabelTooLong
from dns.resolver import NoAnswer, NoNameservers, NXDOMAIN
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.postgres import (  # noqa: E402
    Domain, ARecord, AAAARecord, NSRecord, MXRecord,
    SoaRecord, CNAMERecord
)

# Constants
DEFAULT_PREFETCH = 800
DEFAULT_CONCURRENCY = 500
DEFAULT_TIMEOUT = 1.0

DNS_RECORD_TYPES: Tuple[Tuple[str, str], ...] = (
    ("A", "a_record"),
    ("AAAA", "aaaa_record"),
    ("NS", "ns_record"),
    ("MX", "mx_record"),
    ("SOA", "soa_record"),
    ("CNAME", "cname_record"),
)


def utcnow() -> datetime:
    """Get current UTC datetime without timezone info."""
    return datetime.now(UTC).replace(tzinfo=None)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging for DNS services."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


@dataclass
class WorkerSettings:
    """Settings for DNS worker operations."""
    postgres_dsn: str
    concurrency: int = DEFAULT_CONCURRENCY
    dns_timeout: float = DEFAULT_TIMEOUT
    log_records: bool = False


@dataclass 
class WorkerStats:
    """Statistics tracking for DNS workers."""
    processed: int = 0
    successful: int = 0
    failed: int = 0
    
    def record(self, success: bool) -> None:
        """Record a processed domain."""
        self.processed += 1
        if success:
            self.successful += 1
        else:
            self.failed += 1
            
    def __str__(self) -> str:
        success_rate = (self.successful / self.processed * 100) if self.processed else 0
        return f"Processed: {self.processed}, Success: {self.successful}, Failed: {self.failed}, Rate: {success_rate:.1f}%"


class PostgresAsync:
    """Async PostgreSQL connection manager."""
    
    def __init__(self, postgres_dsn: str) -> None:
        # Convert sync postgresql:// URL to async postgresql+asyncpg://
        if postgres_dsn.startswith("postgresql://"):
            postgres_dsn = postgres_dsn.replace("postgresql://",
                                                "postgresql+asyncpg://")
        elif not postgres_dsn.startswith("postgresql+asyncpg://"):
            # If no scheme, assume we need asyncpg
            if "://" not in postgres_dsn:
                postgres_dsn = f"postgresql+asyncpg://{postgres_dsn}"

        self._engine = create_async_engine(
            postgres_dsn,
            echo=False,
            pool_size=20,  # Increased for better concurrency
            max_overflow=50,  # Higher overflow for burst traffic
            pool_timeout=30,
            pool_recycle=3600,
            pool_pre_ping=True,
            # Performance optimizations
            connect_args={
                "server_settings": {
                    "jit": "off",  # Disable JIT for faster small queries
                    "application_name": "dns_extractor"
                }
            }
        )
        self._session_factory = async_sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
        )

    @asynccontextmanager
    async def get_session(self) -> AsyncIterator[AsyncSession]:
        """Get a database session."""
        async with self._session_factory() as session:
            yield session

    async def close(self) -> None:
        """Close the database connection."""
        await self._engine.dispose()


class DNSRuntime:
    """DNS resolution runtime with PostgreSQL storage."""
    
    def __init__(self, settings: WorkerSettings):
        self.settings = settings
        self.postgres = PostgresAsync(settings.postgres_dsn)
        self.stats = WorkerStats()
        self._resolver: Optional[resolver.Resolver] = None

    async def cleanup(self) -> None:
        """Clean up resources."""
        await self.postgres.close()

    def _should_skip_domain(self, domain: str) -> bool:
        """Skip domains that are likely to fail or waste time."""
        # Skip obviously invalid domains
        if not domain or len(domain) < 3 or len(domain) > 253:
            return True
        
        # Skip domains with invalid characters
        if any(c in domain for c in ['<', '>', '"', '`', ' ', '\t']):
            return True
            
        # Skip localhost and IP addresses
        if domain in ('localhost', '127.0.0.1', '::1'):
            return True
            
        # Skip domains that are likely to be invalid
        invalid_patterns = [
            'xn--', 'www.www.', '-.', '.-', '..',
            'internal', 'local', 'test'
        ]
        if any(pattern in domain for pattern in invalid_patterns):
            return True
            
        return False

    async def process_domain(self, domain: str) -> bool:
        """Process a single domain for DNS records."""
        domain = domain.strip().lower()
        if not domain or self._should_skip_domain(domain):
            return False

        records = await self._resolve_all(domain)
        now = utcnow()

        if records and any(records.values()):
            await self._store_records(domain, records, now)
            if self.settings.log_records:
                self._log_records(domain, records)
            self.stats.record(True)
            return True

        await self._mark_failed(domain, now)
        self.stats.record(False)
        logging.getLogger(__name__).debug("DNS lookup failed for %s", domain)
        return False

    async def _resolve_all(self, domain: str) -> Dict[str, List[object]]:
        """Resolve all DNS record types for a domain."""
        # Use asyncio.gather for better performance
        tasks = [
            self._resolve_record(domain, record_type)
            for record_type, _ in DNS_RECORD_TYPES
        ]
        
        try:
            # Resolve all DNS record types concurrently
            results_list = await asyncio.gather(*tasks, return_exceptions=True)
            
            results: Dict[str, List[object]] = {}
            for i, (_, field_name) in enumerate(DNS_RECORD_TYPES):
                result = results_list[i]
                if isinstance(result, Exception):
                    logging.getLogger(__name__).debug("%s lookup crashed for %s: %s",
                                                      field_name, domain, result)
                    results[field_name] = []
                else:
                    results[field_name] = result or []
                    
            return results
        except Exception as e:
            logging.getLogger(__name__).error("Failed to resolve DNS records for %s: %s", domain, e)
            return {field_name: [] for _, field_name in DNS_RECORD_TYPES}

    async def _resolve_record(self, domain: str,
                              record_type: str) -> List[object]:
        """Resolve a specific DNS record type."""
        timeout = self.settings.dns_timeout

        def _lookup() -> List[object]:
            result: List[object] = []
            # Reuse resolver instance for better performance
            if self._resolver is None:
                self._resolver = resolver.Resolver()
                self._resolver.timeout = timeout
                self._resolver.lifetime = timeout
                # Use Google DNS for better reliability
                self._resolver.nameservers = ['8.8.8.8', '8.8.4.4']
            
            res = self._resolver

            try:
                answers = res.resolve(domain, record_type)
            except (Timeout, LabelTooLong, NoNameservers, EmptyLabel, NoAnswer, NXDOMAIN):
                return result
            except DNSException as e:
                logging.getLogger(__name__).debug("DNS error for %s %s: %s", domain, record_type, e)
                return result

            for item in answers:
                if record_type in ("A", "AAAA"):
                    result.append(item.address.lower())
                elif record_type == "NS":
                    result.append(item.target.to_unicode().strip(".").lower())
                elif record_type == "CNAME":
                    result.append({"target": item.target.to_unicode().strip(".").lower()})
                elif record_type == "MX":
                    result.append({
                        "exchange": item.exchange.to_unicode().lower().strip("."),
                        "preference": item.preference,
                    })
                elif record_type == "SOA":
                    result.append({
                        "mname": item.mname.to_unicode().lower().strip("."),
                        "rname": item.rname.to_unicode().lower().strip("."),
                        "serial": item.serial,
                        "refresh": item.refresh,
                        "retry": item.retry,
                        "expire": item.expire,
                        "minimum": item.minimum,
                    })

            return result

        # Run DNS lookup in thread to avoid blocking
        return await asyncio.to_thread(_lookup)

    async def _store_records(
        self,
        domain: str,
        records: Dict[str, List[object]],
        timestamp: datetime,
    ) -> None:
        """Store DNS records in PostgreSQL."""
        async with self.postgres.get_session() as session:
            try:
                # Find or create domain
                stmt = select(Domain).where(Domain.name == domain)
                result = await session.execute(stmt)
                domain_obj = result.scalar_one_or_none()
                
                if not domain_obj:
                    domain_obj = Domain(
                        name=domain,
                        created_at=timestamp,
                        updated_at=timestamp
                    )
                    session.add(domain_obj)
                    try:
                        await session.flush()  # Get the ID
                    except IntegrityError:
                        # Handle unique constraint violation from another process
                        await session.rollback()
                        # Retry select to get domain inserted by another process
                        result = await session.execute(stmt)
                        domain_obj = result.scalar_one_or_none()
                        if domain_obj is None:
                            raise  # If not found, re-raise original error
                        domain_obj.updated_at = timestamp
                        session.add(domain_obj)
                else:
                    domain_obj.updated_at = timestamp
                    session.add(domain_obj)
                
                # Store DNS records
                await self._store_dns_records(session, domain_obj.id, records)
                await session.commit()
                
            except Exception as e:
                await session.rollback()
                logging.getLogger(__name__).error("Failed to store records for domain %s: %s",
                                                  domain, e)
                raise

    async def _store_dns_records(
        self,
        session,
        domain_id: int,
        records: Dict[str, List[object]]
    ) -> None:
        """Store DNS records in PostgreSQL tables using bulk operations."""
        # Prepare bulk record lists for better performance
        a_records = []
        aaaa_records = []
        ns_records = []
        mx_records = []
        soa_records = []
        cname_records = []
        
        for record_type, record_list in records.items():
            if not record_list:
                continue

            if record_type == "a_record":
                a_records.extend([
                    ARecord(domain_id=domain_id, ip_address=ip)
                    for ip in record_list if ip is not None and str(ip).strip()
                ])
            elif record_type == "aaaa_record":
                aaaa_records.extend([
                    AAAARecord(domain_id=domain_id, ip_address=ip)
                    for ip in record_list if ip is not None and str(ip).strip()
                ])
            elif record_type == "ns_record":
                ns_records.extend([
                    NSRecord(domain_id=domain_id, value=ns)
                    for ns in record_list if ns is not None and str(ns).strip()
                ])
            elif record_type == "mx_record":
                for mx in record_list:
                    if isinstance(mx, dict):
                        exchange_value = mx.get('exchange')
                        if exchange_value and str(exchange_value).strip():
                            mx_records.append(MXRecord(
                                domain_id=domain_id,
                                exchange=str(exchange_value).strip(),
                                priority=mx.get('preference', 0)
                            ))
                    else:
                        mx_str = str(mx).strip()
                        if mx_str:
                            mx_records.append(MXRecord(
                                domain_id=domain_id, exchange=mx_str
                            ))
            elif record_type == "soa_record":
                for soa in record_list:
                    if isinstance(soa, dict):
                        # Serialize SOA dict to string format: "mname rname serial refresh retry expire minimum"
                        soa_value = f"{soa.get('mname', '')} {soa.get('rname', '')} {soa.get('serial', 0)} {soa.get('refresh', 0)} {soa.get('retry', 0)} {soa.get('expire', 0)} {soa.get('minimum', 0)}"
                        if soa_value.strip():
                            soa_records.append(SoaRecord(
                                domain_id=domain_id,
                                value=soa_value.strip()
                            ))
                    else:
                        soa_str = str(soa).strip()
                        if soa_str:
                            soa_records.append(SoaRecord(
                                domain_id=domain_id, 
                                value=soa_str
                            ))
            elif record_type == "cname_record":
                cname_records.extend([
                    CNAMERecord(domain_id=domain_id, target=str(cname.get('target')).strip())
                    for cname in record_list
                    if (
                        (isinstance(cname, dict) and cname.get('target') is not None and str(cname.get('target')).strip()) or
                        (not isinstance(cname, dict) and str(cname).strip())
                    )
                ])
        
        # Bulk add all records at once
        if a_records:
            session.add_all(a_records)
        if aaaa_records:
            session.add_all(aaaa_records)
        if ns_records:
            session.add_all(ns_records)
        if mx_records:
            session.add_all(mx_records)
        if soa_records:
            session.add_all(soa_records)
        if cname_records:
            session.add_all(cname_records)

    async def _mark_failed(self, domain: str, timestamp: datetime) -> None:
        """Mark a domain as having failed DNS lookup."""
        async with self.postgres.get_session() as session:
            try:
                # Find or create domain
                stmt = select(Domain).where(Domain.name == domain)
                result = await session.execute(stmt)
                domain_obj = result.scalar_one_or_none()
                
                if not domain_obj:
                    domain_obj = Domain(
                        name=domain,
                        created_at=timestamp,
                        updated_at=timestamp
                    )
                    session.add(domain_obj)
                    try:
                        await session.flush()
                    except IntegrityError:
                        # Handle unique constraint violation from another process
                        await session.rollback()
                        # Retry select for domain from another process
                        result = await session.execute(stmt)
                        domain_obj = result.scalar_one_or_none()
                        if domain_obj is None:
                            raise  # If not found, re-raise original error
                        domain_obj.updated_at = timestamp
                        session.add(domain_obj)
                else:
                    domain_obj.updated_at = timestamp
                    session.add(domain_obj)
                
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger = logging.getLogger(__name__)
                logger.error("Failed to mark domain as failed %s: %s",
                             domain, e)
                raise

    def _log_records(self, domain: str,
                     records: Dict[str, List[object]]) -> None:
        """Log extracted DNS records."""
        log = logging.getLogger(__name__)
        for record_type, record_list in records.items():
            if record_list:
                log.info("%s: %s -> %s", domain, record_type, record_list)
