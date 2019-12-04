#!/usr/bin/env python3

import os
import sys

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


IP_ADDRESS_FILE_PATH = os.path.join(os.path.dirname(__file__), 'data/')


def load(i):
    with open(IP_ADDRESS_FILE_PATH + 'x' + i, 'r') as f:
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
