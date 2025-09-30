#!/usr/bin/env python3
"""
DNS Worker Service - RabbitMQ Consumer

This service processes DNS resolution jobs from RabbitMQ queues.
It's designed to run as a long-running service that consumes domain
names from a queue and performs DNS record extraction.
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
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Dict, List, Optional

import aio_pika
import click
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import AMQPConnectionError

from shared.models.postgres import (  # noqa: E402
    Domain, ARecord, AAAARecord, NSRecord, MXRecord,
    SoaRecord, CNAMERecord
)

# Import shared DNS components (will create these next)
from tools.dns_shared import (  # noqa: E402
    PostgresAsync, DNSRuntime, WorkerSettings, utcnow,
    configure_logging, DEFAULT_CONCURRENCY, DEFAULT_TIMEOUT,
    DEFAULT_PREFETCH
)
from async_sqlmodel_helpers import resolve_async_dsn

log = logging.getLogger(__name__)
STOP_SENTINEL = b"__STOP__"


@dataclass
class WorkerServiceSettings:
    """Settings for the DNS worker service."""
    postgres_dsn: str
    rabbitmq_url: str
    queue_name: str = "dns_records"
    prefetch: int = DEFAULT_PREFETCH
    concurrency: int = DEFAULT_CONCURRENCY
    dns_timeout: float = DEFAULT_TIMEOUT
    log_records: bool = False
    worker_id: str = "worker-1"


class DNSWorkerService:
    """Long-running DNS worker service that processes RabbitMQ messages."""
    
    def __init__(self, settings: WorkerServiceSettings):
        self.settings = settings
        self.runtime: Optional[DNSRuntime] = None
        self.connection: Optional[aio_pika.abc.AbstractConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractChannel] = None
        self.queue: Optional[aio_pika.abc.AbstractQueue] = None
        self.running = False
        
    async def start(self) -> None:
        """Start the worker service."""
        log.info("Starting DNS worker service %s", self.settings.worker_id)
        
        # Initialize DNS runtime
        worker_settings = WorkerSettings(
            postgres_dsn=self.settings.postgres_dsn,
            concurrency=self.settings.concurrency,
            dns_timeout=self.settings.dns_timeout,
            log_records=self.settings.log_records
        )
        self.runtime = DNSRuntime(worker_settings)
        
        # Connect to RabbitMQ
        await self._connect_rabbitmq()
        
        # Set up signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        self.running = True
        log.info("DNS worker service %s started successfully", 
                 self.settings.worker_id)
        
    async def run(self) -> None:
        """Main service loop."""
        if not self.running:
            await self.start()
            
        try:
            # Start consuming messages
            async with self.queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self.running:
                        break
                        
                    async with message.process():
                        await self._process_message(message)
                        
        except asyncio.CancelledError:
            log.info("Worker service %s cancelled", self.settings.worker_id)
        except Exception as e:
            log.error("Worker service %s error: %s", self.settings.worker_id, e)
            raise
        finally:
            await self.stop()
            
    async def _connect_rabbitmq(self) -> None:
        """Connect to RabbitMQ and set up queue."""
        try:
            self.connection = await aio_pika.connect_robust(
                self.settings.rabbitmq_url
            )
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=self.settings.prefetch)
            
            self.queue = await self.channel.declare_queue(
                self.settings.queue_name, durable=True
            )
            
            log.info("Connected to RabbitMQ queue '%s'", 
                     self.settings.queue_name)
                     
        except Exception as e:
            log.error("Failed to connect to RabbitMQ: %s", e)
            raise
            
    async def _process_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        """Process a single DNS resolution message."""
        try:
            domain = message.body.decode('utf-8')
            
            # Check for stop sentinel
            if message.body == STOP_SENTINEL:
                log.info("Received stop signal, shutting down worker %s", 
                         self.settings.worker_id)
                self.running = False
                return
                
            # Process the domain
            success = await self.runtime.process_domain(domain)
            
            if success:
                log.debug("Successfully processed domain: %s", domain)
            else:
                log.debug("Failed to process domain: %s", domain)
                
        except Exception as e:
            log.error("Error processing message: %s", e)
            
    def _setup_signal_handlers(self) -> None:
        """Set up signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            log.info("Received signal %s, initiating shutdown", signum)
            self.running = False
            
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
    async def stop(self) -> None:
        """Stop the worker service."""
        log.info("Stopping DNS worker service %s", self.settings.worker_id)
        self.running = False
        
        if self.runtime:
            await self.runtime.cleanup()
            
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            
        log.info("DNS worker service %s stopped", self.settings.worker_id)


@click.command()
@click.option("--postgres-dsn", "-p", type=str, required=True,
              help="PostgreSQL connection string")
@click.option("--rabbitmq-url", "-r", type=str, required=True,
              help="RabbitMQ connection URL")
@click.option("--queue-name", "-q", type=str, default="dns_records",
              show_default=True, help="RabbitMQ queue name")
@click.option("--prefetch", type=int, default=DEFAULT_PREFETCH,
              show_default=True, help="RabbitMQ prefetch count")
@click.option("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY,
              show_default=True, help="Concurrent DNS lookups")
@click.option("--dns-timeout", type=float, default=DEFAULT_TIMEOUT,
              show_default=True, help="DNS resolver timeout in seconds")
@click.option("--worker-id", type=str, default="worker-1",
              show_default=True, help="Unique worker identifier")
@click.option("--log-records", is_flag=True,
              help="Log extracted records at info level")
@click.option("--verbose", is_flag=True, help="Enable verbose debug logging")
def main(
    postgres_dsn: str,
    rabbitmq_url: str,
    queue_name: str,
    prefetch: int,
    concurrency: int,
    dns_timeout: float,
    worker_id: str,
    log_records: bool,
    verbose: bool,
) -> None:
    """DNS Worker Service - Process DNS resolution jobs from RabbitMQ."""
    
    log_level = logging.DEBUG if verbose else logging.INFO
    configure_logging(log_level)

    resolved_dsn = resolve_async_dsn(postgres_dsn)

    settings = WorkerServiceSettings(
        postgres_dsn=resolved_dsn,
        rabbitmq_url=rabbitmq_url,
        queue_name=queue_name,
        prefetch=prefetch,
        concurrency=concurrency,
        dns_timeout=dns_timeout,
        worker_id=worker_id,
        log_records=log_records
    )
    
    service = DNSWorkerService(settings)
    
    try:
        asyncio.run(service.run())
    except KeyboardInterrupt:
        log.info("Service interrupted by user")
    except Exception as e:
        log.error("Service failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()