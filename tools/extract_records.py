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


def update_data(db, item, post):
    try:
        return db.dns.update_one({'_id': item['_id']}, {'$set': post}, upsert=False)
    except DuplicateKeyError:
        return


def retrieve_domains(db, skip, limit):
    return db.dns.find({'updated': {'$exists': False}}).sort([('$natural', -1)])[limit - skip:limit]


def get_address(domain, record):
    address_list = []

    try:
        res = resolver.Resolver()
        res.timeout = 1
        res.lifetime = 1
        data = res.query(domain, record)

        for item in data:
            if record != 'MX' and record != 'CNAME':
                address_list.append(item.address)

            elif record == 'CNAME':
                post = {'target': item.target.to_unicode().strip('.')}
                address_list.append(post)

            else:
                post = {'preference': item.preference, 'exchange': item.exchange.to_unicode().lower().strip('.')}
                address_list.append(post)

        return address_list
    except (Timeout, LabelTooLong, NoNameservers, EmptyLabel, NoAnswer, NXDOMAIN):
        return


def worker(host, skip, limit):
    client = connect(args.host)
    db = client.ip_data

    domains = retrieve_domains(db, limit, skip)

    for domain in domains:
        dns = domain['domain']
        now = datetime.utcnow()

        a_record = get_address(dns, 'A')
        mx_record = get_address(dns, 'MX')
        aaaa_record = get_address(dns, 'AAAA')
        cname_record = get_address(dns, 'CNAME')

        if a_record:
            data = update_data(db, domain, {'updated': now, 'a_record': a_record})
            print(u'INFO: updated {}, with {} documents'.format(dns, data.modified_count))

        if aaaa_record:
            data = update_data(db, domain, {'updated': now, 'aaaa_record': aaaa_record})
            print(u'INFO: updated {}, with {} documents'.format(dns, data.modified_count))

        if mx_record:
            data = update_data(db, domain, {'updated': now, 'mx_record': mx_record})
            print(u'INFO: updated {}, with {} documents'.format(dns, data.modified_count))

        if cname_record:
            data = update_data(db, domain, {'updated': now, 'cname_record': cname_record})
            print(u'INFO: updated {}, with {} documents'.format(dns, data.modified_count))

        if not a_record and not aaaa_record and not mx_record and not cname_record:
            update_data(db, domain, {'scan_failed': now})
            print(u'INFO: updated {}, scan failed'.format(dns))

    return


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', help='set worker count', type=int, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    jobs = []
    threads = args.worker
    amount = round(db.dns.estimated_document_count() / (threads + 50000))
    limit = amount

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(args.host, limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        print('exitcode = {}'.format(j.exitcode))
