#!/usr/bin/env python3

import argparse

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


def load(i):
    with open(i) as f:
        return f.readlines()


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


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
    db.ipv4.create_index('ip', unique=True)

    for line in load(args.input):
        try:
            db.ipv4.insert_one({'ip': line.strip()})
            print(line.strip())
        except DuplicateKeyError as e:
            print(e)


if __name__ == '__main__':
    main()
