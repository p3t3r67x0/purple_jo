#!/usr/bin/env python3

import requests

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import WriteError

from requests.exceptions import InvalidURL
from requests.exceptions import ReadTimeout
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ConnectionError

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


def retrieve_domains(db):
    return db.dns.find({'header': {'$exists': False},
                        'header_scan_failed': {'$exists': False}}).sort([('$natural', -1)])


def grab_http_header(domain):
    headers = []

    try:
        r = requests.head(u'https://{}'.format(domain),
                          timeout=1, allow_redirects=False)
    except (InvalidURL, ReadTimeout, ConnectionError, ChunkedEncodingError):
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


def main():
    client = connect()
    db = client.ip_data

    domains = retrieve_domains(db)

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


if __name__ == '__main__':
    main()
