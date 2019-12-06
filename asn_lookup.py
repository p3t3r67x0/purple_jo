#!/usr/bin/env python3

import pyasn
import os

from datetime import datetime
from pymongo import MongoClient


AS_NAMES_FILE_PATH = os.path.join(os.path.dirname(__file__), 'asn_names.json')


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def fetch_all(db):
    return db.lookup.find({'name': {'$exists': False}}).sort([('_id', -1)])


def main():
    client = connect()
    db = client.ip_data

    for i in fetch_all(db):
        res = asn_lookup(i['ip'])

        db.lookup.update_one({'ip': i['ip']}, {'$set': {
                             'updated': datetime.utcnow(),
                             'name': res['name']}}, upsert=False)

        print('INFO: updated document with ip {} and added asn name {}'.format(i['ip'], res['name']))


def asn_lookup(ipv4):
    asndb = pyasn.pyasn('rib.20191127.2000.dat', as_names_file=AS_NAMES_FILE_PATH)
    asn, prefix = asndb.lookup(ipv4)
    name = asndb.get_as_name(asn)

    return {'name': name}


if __name__ == '__main__':
    main()
