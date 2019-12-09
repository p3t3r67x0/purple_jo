#!/usr/bin/env python3

import sys
import json

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


def update_data(db, ip, now, ports):
    try:
        res = db.dns.update_one({'a_record': {'$in': [ip]}}, {'$set': {
                                 'updated': now}, '$addToSet': {'ports': ports}
                                 }, upsert=False)
        if res.modified_count > 0:
            print('INFO: updated ports for ip {} modified {} documents'.format(ip, res.modified_count))
        else:
            print('INFO: nothing to modify for ip {}'.format(ip))
    except DuplicateKeyError:
        pass


if __name__ == '__main__':
    client = connect()
    db = client.ip_data

    document = load_document(sys.argv[1])
    ports = json.loads(document.strip())
    now = datetime.utcnow()

    for port in ports:
        ports.remove(port)

        for p in port['ports']:
            if p:
                data = {'port': p['port'], 'proto': p['proto'],
                        'status': p['status'], 'reason': p['reason']}
                update_data(db, port['ip'], now, data)
