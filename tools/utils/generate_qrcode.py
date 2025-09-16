#!/usr/bin/env python3

import pyqrcode
import multiprocessing
import argparse

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound

from datetime import datetime


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_domains(db, client, skip, limit):
    try:
        return db.dns.find(
            {
                'qrcode': {'$exists': False},
                'domain': {
                    '$regex': r'^(([\w]*\.)?(?!(xn--)+)[\w]*\.[\w]+)$'
                }
            }
        ).sort([('updated', -1)])[limit - skip:limit]
    except KeyboardInterrupt:
        client.close()


def update_data(db, domain, post):
    try:
        res = db.dns.update_one(
            {'domain': domain},
            {'$set': post},
            upsert=False
        )

        if res.modified_count > 0:
            print('INFO: added qrcode for domain {}'.format(domain))
    except DuplicateKeyError:
        return


def generate_qrcode(db, domain, date):
    url = pyqrcode.create(u'https://{}'.format(domain), encoding='utf-8')
    qrcode = url.png_as_base64_str(scale=5, quiet_zone=0)
    update_data(db, domain, {'updated': date, 'qrcode': qrcode})


def worker(host, skip, limit):
    client = connect(host)
    db = client.ip_data
    date = datetime.now()

    try:
        domains = retrieve_domains(db, client, limit, skip)

        for domain in domains:
            generate_qrcode(db, domain['domain'], date)
    except KeyboardInterrupt:
        client.close()
        return
    except CursorNotFound:
        return

    client.close()
    return


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--worker',
        help='set worker count',
        type=int,
        required=True
    )
    parser.add_argument(
        '--host',
        help='set the host',
        type=str,
        required=True
    )
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
    client.close()

    for f in range(threads):
        j = multiprocessing.Process(
            target=worker,
            args=(args.host, limit, amount)
        )
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        print('exitcode = {}'.format(j.exitcode))
