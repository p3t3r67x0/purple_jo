#!/usr/bin/env python3

import sys

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def add_asn(db, asn):
    try:
        now = datetime.utcnow()
        post = {'asn': asn, 'created': now}
        post_id = db.asn.insert_one(post).inserted_id

        print(u'INFO: the asn {} was added with the id {}'.format(asn, post_id))
    except DuplicateKeyError:
        return


def load_asn_file(filename):
    with open(filename, 'r') as f:
        return f.readlines()


def main():
    client = connect()
    db = client.ip_data
    db.asn.create_index('asn', unique=True)

    asns = load_asn_file(sys.argv[1])

    for asn in asns:
        asn = asn.strip()

        print(u'INFO: the asn {} is beeing processed'.format(asn))
        add_asn(db, asn)


if __name__ == '__main__':
    main()
