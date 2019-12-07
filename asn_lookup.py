#!/usr/bin/env python3

import os
import pyasn
import multiprocessing

from datetime import datetime
from pymongo import MongoClient


AS_NAMES_FILE_PATH = os.path.join(os.path.dirname(__file__), 'asn_names.json')


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def retrieve_ips(db, limit, skip):
    return db.lookup.find({'name': {'$exists': False}}).sort([('_id', -1)])[limit - skip:limit]


def asn_lookup(ipv4):
    asndb = pyasn.pyasn('rib.20191127.2000.dat', as_names_file=AS_NAMES_FILE_PATH)
    asn, prefix = asndb.lookup(ipv4)
    name = asndb.get_as_name(asn)

    return {'name': name}


def worker(skip, limit):
    client = connect()
    db = client.ip_data

    for i in retrieve_ips(db, skip, limit):
        res = asn_lookup(i['ip'])

        db.lookup.update_one({'ip': i['ip']}, {'$set': {
                             'updated': datetime.utcnow(),
                             'name': res['name']}}, upsert=False)

        print('INFO: updated document with ip {} and added asn name {}'.format(i['ip'], res['name']))


if __name__ == '__main__':
    client = connect()
    db = client.ip_data

    jobs = []
    threads = 32
    amount = db.lookup.estimated_document_count() / threads
    limit = amount

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        print('exitcode = {}'.format(j.exitcode))
