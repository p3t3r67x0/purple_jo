#!/usr/bin/env python3

import socket

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def update_data(db, doc_id, ip, post):
    try:
        db.dns.update_one({'_id': doc_id}, {'$set': post}, upsert=False)
        print(u'INFO: updated ip {} banner'.format(ip))
    except DuplicateKeyError:
        pass


def retrieve_ips(db):
    return db.dns.find({'a_record': {'$exists': True},
                        'banner': {'$exists': False},
                        'banner_scan_failed': {'$exists': False}}).sort([('$natural', -1)])


def grab_banner(ip, port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((ip, port))
        banner = s.recv(1024)
        s.close()
        return str(banner.decode('utf-8').strip())
    except Exception:
        return ''


def main():
    client = connect()
    db = client.ip_data

    for record in retrieve_ips(db):
        for ip in record['a_record']:
            banner = grab_banner(ip, 22)

            if banner:
                print(banner)
                update_data(db, record['_id'], ip, {'banner': banner, 'updated': datetime.utcnow()})
            else:
                update_data(db, record['_id'], ip, {'banner_scan_failed': datetime.utcnow()})


if __name__ == '__main__':
    main()
