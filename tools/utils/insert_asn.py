#!/usr/bin/env python3

import argparse

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from datetime import datetime


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


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


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help='set input file name', type=str, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


def main():
    args = argparser()
    client = connect(args.host)
    db = client.ip_data
    db.asn.create_index('asn', unique=True)

    asns = load_asn_file(args.input)

    for asn in asns:
        asn = asn.strip()

        print(u'INFO: the asn {} is beeing processed'.format(asn))
        add_asn(db, asn)


if __name__ == '__main__':
    main()
