#!/usr/bin/env python3

import multiprocessing
import argparse

from dns import resolver
from dns.name import EmptyLabel
from dns.name import LabelTooLong
from dns.resolver import NoAnswer
from dns.resolver import NXDOMAIN
from dns.resolver import NoNameservers
from dns.exception import Timeout

from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def update_data(db, domain, date, type, record):
    try:
        return db.dns.update_one({'domain': domain}, {'$set': {'updated': date},
                                                      '$addToSet': {type: record}}, upsert=False)
    except DuplicateKeyError:
        return


def update_failed(db, type, domain, post):
    if type is not None:
        db.dns.update_one({type: domain}, {'$set': post}, upsert=False)


def add_data(db, domain, post):
    try:
        post['domain'] = domain.lower()
        post['created'] = datetime.utcnow()
        post_id = db.dns.insert_one(post)
    except DuplicateKeyError:
        return


def retrieve_domains(db, skip, limit):
    return db.dns.find({'updated': {'$exists': False}}).sort([('$natural', -1)])[limit - skip:limit]


def retrieve_records(domain, record):
    records = []

    try:
        res = resolver.Resolver()
        res.timeout = 1
        res.lifetime = 1
        items = res.query(domain, record)

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
                post = {'preference': item.preference,
                        'exchange': item.exchange.to_unicode().lower().strip('.')}
                records.append(post)

        return records
    except (Timeout, LabelTooLong, NoNameservers, EmptyLabel, NoAnswer, NXDOMAIN):
        return


def handle_records(db, domain, date, type=None, record=None):
    a_records = retrieve_records(domain, 'A')
    ns_records = retrieve_records(domain, 'NS')
    mx_records = retrieve_records(domain, 'MX')
    soa_records = retrieve_records(domain, 'SOA')
    aaaa_records = retrieve_records(domain, 'AAAA')
    cname_records = retrieve_records(domain, 'CNAME')

    if a_records:
        for a_record in a_records:
            data = update_data(db, domain, date, 'a_record', a_record)

            if data.modified_count == 0:
                add_data(db, domain, {'a_record': a_records})
            else:
                print(u'INFO: updated {}, A record with {}'.format(domain, a_record))

    if aaaa_records:
        for aaaa_record in aaaa_records:
            data = update_data(db, domain, date, 'aaaa_record', aaaa_record)

            if data.modified_count == 0:
                add_data(db, domain, {'aaaa_record': aaaa_records})
            else:
                print(u'INFO: updated {}, AAAA record with {}'.format(domain, aaaa_record))

    if ns_records:
        for ns_record in ns_records:
            data = update_data(db, domain, date, 'ns_record', ns_record)

            if data.modified_count == 0:
                add_data(db, domain, {'ns_record': ns_records})
            else:
                print(u'INFO: updated {}, NS record with {}'.format(domain, ns_record))

    if mx_records:
        for mx_record in mx_records:
            data = update_data(db, domain, date, 'mx_record', mx_record)

            if data.modified_count == 0:
                add_data(db, domain, {'mx_record': mx_records})
            else:
                print(u'INFO: updated {}, MX record with {}'.format(domain, mx_record))

    if soa_records:
        for soa_record in soa_records:
            data = update_data(db, domain, date, 'soa_record', soa_record)

            if data.modified_count == 0:
                add_data(db, domain, {'soa_record': soa_records})
            else:
                print(u'INFO: updated {}, SOA record with {}'.format(domain, soa_record))

    if cname_records:
        for cname_record in cname_records:
            data = update_data(db, domain, date, 'cname_record', cname_record)

            if data.modified_count == 0:
                add_data(db, domain, {'cname_record': cname_records})
            else:
                print(u'INFO: updated {}, CNAME record with {}'.format(domain, cname_record))

    if not any([a_records, aaaa_records, mx_records, ns_records, soa_records, cname_records]):
        update_failed(db, type, domain, {record: datetime.utcnow()})
        print(u'INFO: coud not find any records for domain {}'.format(domain))


def worker(host, skip, limit):
    client = connect(args.host)
    db = client.ip_data

    domains = retrieve_domains(db, limit, skip)

    for domain in domains:
        handle_records(db, domain['domain'], datetime.utcnow())

    client.close()
    return


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', help='set worker count',
                        type=int, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    jobs = []
    threads = args.worker
    amount = round(db.dns.estimated_document_count() / threads)
    limit = amount

    for f in range(threads):
        j = multiprocessing.Process(
            target=worker, args=(args.host, limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        client.close()
        print('exitcode = {}'.format(j.exitcode))
