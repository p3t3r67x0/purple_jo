#!/usr/bin/env python3

import sys
import json
import argparse

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


if __name__ == '__main__':
    client = connect()
    db = client.ip_data
    args = argparser()

    if args.collection == 'lookup':
        col = 'lookup'
    elif args.collection == 'dns':
        col = 'dns'

    document = load_document(args.input)
    ports = json.loads(document.strip())
    now = datetime.utcnow()

    for port in ports:
        ports.remove(port)

        for p in port['ports']:
            if p:
                data = {'port': p['port'], 'proto': p['proto'],
                        'status': p['status'], 'reason': p['reason']}
                update_data(db, col, port['ip'], now, data)
