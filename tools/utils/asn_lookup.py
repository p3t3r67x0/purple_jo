#!/usr/bin/env python3

import os
import pyasn
import multiprocessing
import argparse

from datetime import datetime
from pymongo import MongoClient


AS_NAMES_FILE_PATH = os.path.join(os.path.dirname(__file__), 'asn_names.json')


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_ips(db, limit, skip):
    return db.lookup.find({'name': {'$exists': False}}).sort([('_id', -1)])[limit - skip:limit]


def asn_lookup(ipv4):
    asndb = pyasn.pyasn('rib.20191127.2000.dat', as_names_file=AS_NAMES_FILE_PATH)
    asn, prefix = asndb.lookup(ipv4)
    name = asndb.get_as_name(asn)

    return {'name': name}


def worker(host, skip, limit):
    client = connect(host)
    db = client.ip_data

    for i in retrieve_ips(db, skip, limit):
        res = asn_lookup(i['ip'])

        db.lookup.update_one({'ip': i['ip']}, {'$set': {
                             'updated': datetime.utcnow(),
                             'name': res['name']}}, upsert=False)

        print('INFO: updated document with ip {} and added asn name {}'.format(i['ip'], res['name']))

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
    amount = round(db.lookup.estimated_document_count() / (threads + 50000))
    limit = amount

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(args.host, limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        client.close()
        print('exitcode = {}'.format(j.exitcode))
