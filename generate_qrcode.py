#!/usr/bin/env python3

import pyqrcode
import multiprocessing

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound

from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def retrieve_domains(db, skip, limit):
    return db.dns.find({'qrcode': {'$exists': False}}).sort([('updated', -1)])[limit - skip:limit]


def update_data(db, domain, post):
    try:
        res = db.dns.update_one({'domain': domain}, {'$set': post}, upsert=False)

        if res.modified_count > 0:
            print('INFO: added qrcode for domain {}'.format(domain))
    except DuplicateKeyError:
        pass


def generate_qrcode(id, domain):
    url = pyqrcode.create('https://{}'.format(domain.encode('utf-8')))
    return url.png_as_base64_str(scale=5, quiet_zone=0)


def worker(skip, limit):
    client = connect()
    db = client.ip_data
    now = datetime.utcnow()

    try:
        domains = retrieve_domains(db, limit, skip)
    except CursorNotFound:
        return

    for domain in domains:
        qrcode = generate_qrcode(domain['_id'], domain['domain'])
        update_data(db, domain['domain'], {'updated': now, 'qrcode': qrcode})

    return


if __name__ == '__main__':
    client = connect()
    db = client.ip_data

    jobs = []
    threads = 96
    amount = round(db.dns.estimated_document_count() / (threads + 500000))
    limit = amount

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        print('exitcode = {}'.format(j.exitcode))
