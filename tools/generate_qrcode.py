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
        return db.dns.find({'qrcode': {'$exists': False}, 'domain': {
                            '$regex': '^(([\w]*\.)?(?!(xn--)+)[\w]*\.[\w]+)$'}}).sort(
                           [('updated', -1)])[limit - skip:limit]
    except KeyboardInterrupt:
        client.close()


def update_data(db, client, domain, post):
    try:
        res = db.dns.update_one({'domain': domain}, {'$set': post}, upsert=False)

        if res.modified_count > 0:
            print('INFO: added qrcode for domain {}'.format(domain))
    except KeyboardInterrupt:
        client.close()
        return
    except DuplicateKeyError:
        return


def generate_qrcode(id, domain):
    url = pyqrcode.create('https://{}'.format(domain))
    return url.png_as_base64_str(scale=5, quiet_zone=0)


def worker(host, skip, limit):
    client = connect(host)
    db = client.ip_data
    now = datetime.utcnow()

    try:
        domains = retrieve_domains(db, client, limit, skip)

        for domain in domains:
            qrcode = generate_qrcode(domain['_id'], domain['domain'])
            update_data(db, client, domain['domain'], {'updated': now, 'qrcode': qrcode})
    except KeyboardInterrupt:
        client.close()
        return
    except CursorNotFound:
        return

    client.close()
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
    amount = round(db.dns.estimated_document_count() / threads)
    limit = amount
    client.close()

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(args.host, limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        print('exitcode = {}'.format(j.exitcode))
