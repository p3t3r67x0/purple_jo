#!/usr/bin/env python3

import sys

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


def load(i):
    with open(i) as f:
        return f.readlines()


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def main():
    client = connect()
    db = client.ip_data
    db.ipv4.create_index('ip', unique=True)

    for line in load(sys.argv[1]):
        try:
            db.ipv4.insert_one({'ip': line.strip()})
            print(line.strip())
        except DuplicateKeyError as e:
            print(e)


if __name__ == '__main__':
    main()
