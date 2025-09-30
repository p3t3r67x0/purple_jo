#!/usr/bin/env python3
"""
Domain Publisher Service - RabbitMQ Producer

This service publishes domain names to RabbitMQ queues for DNS processing.
It queries the database for unprocessed domains and feeds them to worker services.
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
from typing import AsyncIterator

import aio_pika
import click
from aio_pika import DeliveryMode, Message
from sqlmodel import select, func

from shared.models.postgres import Domain, ARecord  # noqa: E402

# Import shared components (will create these next)
from tools.dns_shared import (  # noqa: E402
    PostgresAsync, configure_logging
)
from async_sqlmodel_helpers import resolve_async_dsn

log = logging.getLogger(__name__)
STOP_SENTINEL = b"__STOP__"


class DomainPublisher:
    """Service that publishes domains to RabbitMQ for DNS processing."""
    
    def __init__(self, postgres_dsn: str, rabbitmq_url: str):
        self.postgres = PostgresAsync(postgres_dsn)
        self.rabbitmq_url = rabbitmq_url
        
    async def count_pending_domains(self) -> int:
        """Count domains that need DNS processing."""
        async with self.postgres.get_session() as session:
            # Count domains that don't have any A records processed yet
            stmt = select(func.count(Domain.id)).outerjoin(ARecord).where(
                ARecord.domain_id.is_(None)
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none() or 0
            
    async def iter_pending_domains(self, batch_size: int = 10_000) -> AsyncIterator[str]:
        """Stream domains that need DNS processing."""
        offset = 0
        
        while True:
            async with self.postgres.get_session() as session:
                # Get domains that don't have any A records processed yet
                stmt = select(Domain.name).outerjoin(ARecord).where(
                    ARecord.domain_id.is_(None)
                ).offset(offset).limit(batch_size)

                result = await session.execute(stmt)
                domains = result.scalars().all()
                
                if not domains:
                    break
                    
                for domain in domains:
                    yield domain
                    
                offset += len(domains)
                
                # If we got fewer than batch_size, we're done
                if len(domains) < batch_size:
                    break
                    
    async def publish_domains(
        self,
        queue_name: str = "dns_records",
        batch_size: int = 10_000,
        worker_count: int = 1,
        purge_queue: bool = False
    ) -> int:
        """Publish domains to RabbitMQ queue."""
        
        connection = await aio_pika.connect_robust(self.rabbitmq_url)
        channel = await connection.channel()
        
        try:
            # Declare queue
            queue = await channel.declare_queue(queue_name, durable=True)
            
            # Purge queue if requested
            if purge_queue:
                await queue.purge()
                log.info("Purged queue '%s'", queue_name)
                
            # Get exchange
            exchange = channel.default_exchange
            
            published = 0
            
            # Publish domains
            async for domain in self.iter_pending_domains(batch_size):
                message = Message(
                    domain.encode("utf-8"),
                    delivery_mode=DeliveryMode.PERSISTENT
                )
                await exchange.publish(message, routing_key=queue_name)
                published += 1
                
                if published % 10000 == 0:
                    log.info("Published %d domains so far...", published)
                    
            # Send stop messages for workers
            for _ in range(worker_count):
                stop_message = Message(
                    STOP_SENTINEL,
                    delivery_mode=DeliveryMode.PERSISTENT
                )
                await exchange.publish(stop_message, routing_key=queue_name)
                
            log.info("Published %d domains to queue '%s'", published, queue_name)
            return published
            
        finally:
            await connection.close()
            
    async def cleanup(self) -> None:
        """Clean up resources."""
        await self.postgres.close()


@click.command()
@click.option("--postgres-dsn", "-p", type=str, required=True,
              help="PostgreSQL connection string")
@click.option("--rabbitmq-url", "-r", type=str, required=True,
              help="RabbitMQ connection URL")
@click.option("--queue-name", "-q", type=str, default="dns_records",
              show_default=True, help="RabbitMQ queue name")
@click.option("--batch-size", type=int, default=10000,
              show_default=True, help="Batch size for domain queries")
@click.option("--worker-count", "-w", type=int, default=1,
              show_default=True, help="Number of workers to send stop signals to")
@click.option("--purge-queue/--no-purge-queue", default=True,
              help="Purge existing messages in queue before publishing")
@click.option("--count-only", is_flag=True,
              help="Only count pending domains, don't publish")
@click.option("--verbose", is_flag=True, help="Enable verbose debug logging")
def main(
    postgres_dsn: str,
    rabbitmq_url: str,
    queue_name: str,
    batch_size: int,
    worker_count: int,
    purge_queue: bool,
    count_only: bool,
    verbose: bool,
) -> None:
    """Domain Publisher Service - Queue domains for DNS processing."""
    
    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)
    
    resolved_dsn = resolve_async_dsn(postgres_dsn)

    async def run():
        publisher = DomainPublisher(resolved_dsn, rabbitmq_url)
        
        try:
            # Count pending domains
            total_domains = await publisher.count_pending_domains()
            click.echo(f"[INFO] Total domains to process: {total_domains}")
            
            if total_domains == 0:
                click.echo("[INFO] No domains to process")
                return
                
            if count_only:
                click.echo("[INFO] Count-only mode, not publishing")
                return
                
            # Publish domains
            published = await publisher.publish_domains(
                queue_name=queue_name,
                batch_size=batch_size,
                worker_count=worker_count,
                purge_queue=purge_queue
            )
            
            click.echo(f"[INFO] Successfully published {published} domains")
            
        finally:
            await publisher.cleanup()
    
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Publisher interrupted by user")
    except Exception as e:
        log.error("Publisher failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()