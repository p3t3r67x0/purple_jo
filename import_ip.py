#!/usr/bin/env python3

import os
import socket

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


IP_ADDRESS_FILE_PATH = os.path.join(os.path.dirname(__file__), 'data/')


def load(i):
    with open(IP_ADDRESS_FILE_PATH + 'x' + i, 'r') as f:
        return f.readlines()


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def main():
    db = connect()
    col = db.ip_data
    col.ipv4.create_index('ip', unique=True)

    for i in range(0, 1):
        for line in load('{:02d}'.format(i)):
            try:
                host = socket.gethostbyaddr(line.strip())[0]
            except Exception as e:
                host = None

            try:
                col.ipv4.insert_one({'ip': line.strip(), 'host': host})
            except DuplicateKeyError as e:
                print(e)

            print(line.strip(), host)


if __name__ == '__main__':
    main()
