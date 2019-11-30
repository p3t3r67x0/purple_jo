#!/usr/bin/env python3

import pyasn
import socket
import os

from datetime import datetime
from pymongo import MongoClient


AS_NAMES_FILE_PATH = os.path.join(os.path.dirname(__file__), 'asn_names.json')


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def fetch_all(col):
    return col.ipv4.find({'as': {'$exists': False}}, {'_id': 0}).sort([('_id', -1)])


def main():
    db = connect()
    col = db.ip_data
    for i in fetch_all(col):
        try:
            host = socket.gethostbyaddr(i['ip'])[0]
        except Exception as e:
            host = None

        res = asn_lookup(i['ip'])
        col.ipv4.update_one({'ip': i['ip']}, {'$set': {'host': host}, '$push': { 'as': res }}, upsert=False)

        print(i['ip'], res, host)


def asn_lookup(ipv4):
    asndb = pyasn.pyasn('rib.20191127.2000.dat', as_names_file=AS_NAMES_FILE_PATH)
    asn, prefix = asndb.lookup(ipv4)
    name = asndb.get_as_name(asn)

    return {'prefix': prefix, 'name': name, 'asn': asn, 'created': datetime.utcnow()}


if __name__ == '__main__':
    main()
