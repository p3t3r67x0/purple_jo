#!/usr/bin/env python3

import pyqrcode

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def retrieve_domains(db):
    return db.dns.find({'qrcode': {'$exists': False}}).sort([('updated', -1)])


def update_data(db, domain, post):
    try:
        res = db.dns.update_one({'domain': domain}, {'$set': post}, upsert=False)

        if res.modified_count > 0:
            print('INFO: added qrcode for domain {}'.format(domain))
    except DuplicateKeyError:
        pass


def generate_qrcode(id, domain):
    url = pyqrcode.create(u'https://{}'.format(domain))
    return url.png_as_base64_str(scale=5, quiet_zone=0)


def main():
    client = connect()
    db = client.ip_data
    now = datetime.utcnow()

    for domain in retrieve_domains(db):
        qrcode = generate_qrcode(domain['_id'], domain['domain'])
        update_data(db, domain['domain'], {'updated': now, 'qrcode': qrcode})


if __name__ == '__main__':
    main()
