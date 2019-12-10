#!/usr/bin/env python3

import sys
import json
import argparse
import multiprocessing

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from datetime import datetime


def load_document(filename):
    try:
        with open(filename, 'r') as f:
            return f.read()
    except IOError:
        sys.exit(1)


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def update_data(db, col, ip, now, ports):
    try:
        if col == 'lookup':
            res = db.lookup.update_many({'ip': ip}, {'$set': {'updated': now}, '$addToSet': {
                                       'ports': ports}}, upsert=False)
        elif col == 'dns':
            res = db.dns.update_many({'a_record': {'$in': [ip]}}, {'$set': {
                                       'updated': now}, '$addToSet': {'ports': ports}
                                       }, upsert=False)
        if res.modified_count > 0:
            print('INFO: updated ports for ip {} modified {} documents'.format(ip, res.modified_count))
        else:
            if col == 'lookup':
                res = db.lookup.insert_one({'ip': ip, 'ports': [ports]})
                print('INFO: certated document for ip {} with id {}'.format(ip, res.inserted_id))
            else:
                print('INFO: nothing to modify for ip {}'.format(ip))
    except DuplicateKeyError:
        pass


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--collection', '-c', help='set collection to update')
    parser.add_argument('--input', '-i', help='set input file name')
    args = parser.parse_args()

    return args


def worker(col, ports):
    client = connect()
    db = client.ip_data

    for port in ports:
        ports.remove(port)

        for p in port['ports']:
            if p:
                data = {'port': p['port'], 'proto': p['proto'],
                        'status': p['status'], 'reason': p['reason']}
                update_data(db, col, port['ip'], datetime.utcnow(), data)

    return


if __name__ == '__main__':
    jobs = []
    threads = 64
    args = argparser()
    document = load_document(args.input)
    ports = json.loads(document.strip())
    amount = round(len(ports) / threads)
    limit = amount
    print(limit, amount)

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(args.collection, (ports[limit - amount:limit]),))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        print('exitcode = {}'.format(j.exitcode))
