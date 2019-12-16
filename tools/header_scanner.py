#!/usr/bin/env python3

import requests
import multiprocessing

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound
from pymongo.errors import WriteError

from requests.exceptions import InvalidURL
from requests.exceptions import ReadTimeout
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ConnectionError

from urllib3.exceptions import InvalidHeader
from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def update_data(db, doc_id, domain, post):
    try:
        db.dns.update_one({'_id': doc_id}, {'$set': post}, upsert=False)
        print(u'INFO: updated domain {} header'.format(domain))
    except WriteError:
        db.dns.update_one({'_id': doc_id},
                          {'$set': {'header_scan_failed': datetime.utcnow()}}, upsert=False)
    except DuplicateKeyError:
        pass


def retrieve_domains(db, skip, limit):
    return db.dns.find({'header': {'$exists': False},
                        'header_scan_failed': {'$exists': False},
                        'ports.port': {'$in': [80, 443]}})[limit - skip:limit]


def grab_http_header(domain):
    headers = []

    try:
        r = requests.head(u'https://{}'.format(domain),
                          timeout=1, allow_redirects=False)
    except (InvalidHeader, InvalidURL, ReadTimeout, ConnectionError, ChunkedEncodingError):
        return

    try:
        http = {'version': '{}.{}'.format(
            str(r.raw.version)[0], str(r.raw.version)[1])}
        status = {'status': '{}'.format(r.status_code)}
        headers = {k.lower(): v for k, v in r.headers.items()}
        headers.update(status)
        headers.update(http)

        return headers
    except UnicodeDecodeError:
        return


def worker(skip, limit):
    client = connect()
    db = client.ip_data

    try:
        domains = retrieve_domains(db, limit, skip)
    except CursorNotFound:
        return

    for domain in domains:
        print(u'INFO: scanning {} header'.format(domain['domain']))

        header = grab_http_header(domain['domain'])

        print(header)
        if header:
            update_data(db, domain['_id'], domain['domain'], {
                        'header': header, 'updated': datetime.utcnow()})
        else:
            update_data(db, domain['_id'], domain['domain'],
                        {'header_scan_failed': datetime.utcnow()})

    return


if __name__ == '__main__':
    client = connect()
    db = client.ip_data

    jobs = []
    threads = 96
    amount = round(db.dns.estimated_document_count() / (threads + 50000))
    limit = amount

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        print('exitcode = {}'.format(j.exitcode))
