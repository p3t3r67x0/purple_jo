#!/usr/bin/env python3

import logging
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

from dns import resolver
from dns.name import EmptyLabel
from dns.name import LabelTooLong
from dns.resolver import NoAnswer
from dns.resolver import NXDOMAIN
from dns.resolver import NoNameservers
from dns.exception import Timeout

from datetime import datetime

from pymongo import MongoClient, ReturnDocument

import click


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


DNS_RECORD_TYPES = (
    ('A', 'a_record'),
    ('AAAA', 'aaaa_record'),
    ('NS', 'ns_record'),
    ('MX', 'mx_record'),
    ('SOA', 'soa_record'),
    ('CNAME', 'cname_record'),
)


DEFAULT_CLAIM_BATCH_SIZE = 100


logger = logging.getLogger("extract_records")


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="[%(levelname)s] %(processName)s %(message)s",
    )


def claim_domains(db, batch_size: int):
    """Atomically claim a batch of unprocessed domains for this worker."""
    claimed = []

    for _ in range(batch_size):
        doc = db.dns.find_one_and_update(
            {
                'updated': {'$exists': False},
                'claimed': {'$ne': True},
            },
            {
                '$set': {'claimed': True},
            },
            sort=[('_id', 1)],
            return_document=ReturnDocument.AFTER,
        )

        if not doc:
            break

        claimed.append(doc)

    if claimed:
        logger.debug("claimed %s domains", len(claimed))

    return claimed


def retrieve_records(domain, record):
    records = []

    try:
        res = resolver.Resolver()
        res.timeout = 1
        res.lifetime = 1
        items = res.resolve(domain, record)

        for item in items:
            if record not in ['MX', 'NS', 'SOA', 'CNAME']:
                records.append(item.address)
            elif record == 'NS':
                records.append(item.target.to_unicode().strip('.').lower())
            elif record == 'SOA':
                if len(records) > 0:
                    records[0] = item.to_text().replace('\\', '').lower()
                else:
                    records.append(item.to_text().replace('\\', '').lower())
            elif record == 'CNAME':
                post = {'target': item.target.to_unicode().strip('.').lower()}
                records.append(post)
            else:
                post = {
                    'preference': item.preference,
                    'exchange': item.exchange.to_unicode().lower().strip('.')
                }
                records.append(post)

        return records
    except (Timeout, LabelTooLong, NoNameservers, EmptyLabel, NoAnswer, NXDOMAIN):
        return


def handle_records(db, domain, date, executor):
    normalized_domain = domain.lower()

    record_results = {}

    futures = {
        executor.submit(retrieve_records, domain, record_type): field_name
        for record_type, field_name in DNS_RECORD_TYPES
    }

    for future in as_completed(futures):
        field_name = futures[future]
        records = future.result()
        if records:
            record_results[field_name] = records
            logger.debug("retrieved %s records for %s", field_name, domain)

    if record_results:
        update_doc = {
            '$set': {'updated': date},
            '$setOnInsert': {'created': date, 'domain': normalized_domain},
            '$unset': {'claimed': ''},
        }

        add_to_set = {
            field: {'$each': values}
            for field, values in record_results.items()
        }

        if add_to_set:
            update_doc['$addToSet'] = add_to_set

        db.dns.update_one({'domain': normalized_domain}, update_doc, upsert=True)
        logger.info("updated DNS records for %s", domain)
    else:
        db.dns.update_one(
            {'domain': normalized_domain},
            {
                '$set': {
                    'updated': date,
                    'dns_lookup_failed': date,
                },
                '$setOnInsert': {'created': date, 'domain': normalized_domain},
                '$unset': {'claimed': ''},
            },
            upsert=True,
        )
        logger.warning("no DNS records found for %s", domain)


def worker(host: str, claim_batch_size: int, dns_threads: int, verbose: bool):
    configure_logging(verbose)
    logger.info("worker starting")
    client = connect(host)
    db = client.ip_data

    max_workers = dns_threads or len(DNS_RECORD_TYPES)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            domains = claim_domains(db, claim_batch_size)
            if not domains:
                logger.debug("no more domains to claim")
                break

            for domain in domains:
                domain_name = domain.get('domain')
                if not domain_name:
                    db.dns.update_one(
                        {'_id': domain['_id']},
                        {
                            '$set': {'updated': datetime.now()},
                            '$unset': {'claimed': ''},
                        },
                        upsert=False,
                    )
                    continue

                claimed_at = datetime.now()
                logger.info("processing %s", domain_name)
                handle_records(db, domain_name, claimed_at, executor)

    client.close()
    logger.info("worker finished")
    return


@click.command()
@click.option('--host', '-h', type=str, required=True, help='MongoDB host to connect to')
@click.option('--workers', '-w', type=int, default=4, show_default=True, help='Number of worker processes')
@click.option('--claim-batch-size', '--batch', type=int, default=DEFAULT_CLAIM_BATCH_SIZE, show_default=True, help='Number of domains each worker claims per iteration')
@click.option('--dns-threads', type=int, default=0, show_default=True, help='Thread count for concurrent DNS lookups (0 = auto)')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def main(host: str, workers: int, claim_batch_size: int, dns_threads: int, verbose: bool) -> None:
    """Extract DNS records for domains stored in MongoDB."""

    configure_logging(verbose)
    logger.info(
        "starting extractor with %s workers, claim batch %s, dns threads %s",
        workers,
        claim_batch_size,
        dns_threads or len(DNS_RECORD_TYPES),
    )

    client = connect(host)
    db = client.ip_data

    pending_filter = {'updated': {'$exists': False}, 'claimed': {'$ne': True}}
    pending_total = db.dns.count_documents(pending_filter)
    logger.info("pending domains to process: %s", pending_total)

    processes = []
    for idx in range(workers):
        process = multiprocessing.Process(
            target=worker,
            name=f"worker-{idx + 1}",
            args=(host, claim_batch_size, dns_threads, verbose),
        )
        process.start()
        processes.append(process)
        logger.debug("started %s (pid=%s)", process.name, process.pid)

    for process in processes:
        process.join()
        logger.info("%s exited with code %s", process.name, process.exitcode)

    client.close()
    logger.info("extractor finished")


if __name__ == '__main__':
    main()
