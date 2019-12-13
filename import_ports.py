#!/usr/bin/env python3

import sys
import json
import argparse
import multiprocessing
import time

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import AutoReconnect

from json.decoder import JSONDecodeError
from datetime import datetime


def load_document(filename):
    try:
        with open(filename, 'r') as f:
            return f.readlines()
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
    except AutoReconnect:
        time.sleep(30)
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

        try:
            p = json.loads(port.strip().strip(',').encode("utf-8"))
        except JSONDecodeError:
            return

        r = p['ports'][0]

        if r:
            data = {'port': r['port'], 'proto': r['proto'],
                    'status': r['status'], 'reason': r['reason']}

            print(data)
                update_data(db, col, p['ip'], datetime.utcnow(), data)

    return


if __name__ == '__main__':
    jobs = []
    threads = 64
    args = argparser()
    records = load_document(args.input)
    amount = round(len(records) / threads)

    if amount < 1:
        amount = 1

    limit = amount
    print(limit, amount)

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(args.collection, (records[limit - amount:limit]),))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        print('exitcode = {}'.format(j.exitcode))
