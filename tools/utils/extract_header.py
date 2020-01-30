#!/usr/bin/env python3

import requests
import multiprocessing
import argparse

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound
from pymongo.errors import WriteError

from requests.exceptions import InvalidURL
from requests.exceptions import ReadTimeout
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ConnectionError

from urllib3.exceptions import InvalidHeader
from fake_useragent import UserAgent
from datetime import datetime


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def update_data(db, domain, post):
    try:
        data = db.dns.update_one({'domain': domain}, {
                                 '$set': post}, upsert=False)

        if data.modified_count > 0:
            print(u'INFO: updated domain {} header'.format(domain))
    except WriteError:
        db.dns.update_one({'domain': domain},
                          {'$set': {'header_scan_failed': datetime.utcnow()}}, upsert=False)
    except DuplicateKeyError:
        pass


def retrieve_domains(db, skip, limit):
    return db.dns.find({'header': {'$exists': False},
                        'header_scan_failed': {'$exists': False},
                        'ports.port': {'$in': [80, 443]}})[limit - skip:limit]


def extract_header(db, domain, date):
    ua = UserAgent()
    headers = []

    try:
        h = {'User-Agent': ua.random}
        r = requests.head(u'http://{}'.format(domain),
                          timeout=1, allow_redirects=False, headers=h)
    except (InvalidHeader, InvalidURL, ReadTimeout, ConnectionError, ChunkedEncodingError):
        update_data(db, domain, {'header_scan_failed': date})
        return

    try:
        http = {'version': '{}.{}'.format(
            str(r.raw.version)[0], str(r.raw.version)[1])}
        status = {'status': '{}'.format(r.status_code)}
        headers = {k.lower(): v for k, v in r.headers.items()}
        headers.update(status)
        headers.update(http)
    except UnicodeDecodeError:
        return

    if headers:
        update_data(db, domain, {'header': headers, 'updated': date})
    else:
        update_data(db, domain, {'header_scan_failed': date})


def worker(host, skip, limit):
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    try:
        domains = retrieve_domains(db, limit, skip)
    except CursorNotFound:
        return

    for domain in domains:
        print(u'INFO: scanning {} header'.format(domain['domain']))
        extract_header(db, domain['domain'], datetime.utcnow())

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
    amount = round(db.dns.estimated_document_count() / (threads + 50000))
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
