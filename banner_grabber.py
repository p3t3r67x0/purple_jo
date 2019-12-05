#!/usr/bin/env python3

import socket
import multiprocessing

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def update_data(db, doc_id, domain, ip, post):
    try:
        db.dns.update_one({'_id': doc_id}, {'$set': post}, upsert=False)
        print(u'INFO: updated domain {} with ip {} banner'.format(domain, ip))
    except DuplicateKeyError:
        pass


def retrieve_documents(db, skip, limit):
    return db.dns.find({'a_record': {'$exists': True},
                        'banner': {'$exists': False},
                        'banner_scan_failed': {'$exists': False}}).sort(
                        [('updated', -1)])[limit - skip:limit]


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


def worker(skip, limit):
    client = connect()
    db = client.ip_data

    for document in retrieve_documents(db, limit, skip):
        banner = grab_banner(document['a_record'][0], 22)

        if banner:
            print(banner)
            update_data(db, document['_id'], document['domain'], document['a_record'][0],
                        {'banner': banner, 'updated': datetime.utcnow()})
        else:
            update_data(db, document['_id'], document['domain'], document['a_record'][0],
                        {'banner_scan_failed': datetime.utcnow()})

    return


def main():
    client = connect()
    db = client.ip_data

    jobs = []
    threads = 16
    amount = db.dns.count_documents({}) / threads
    limit = amount

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        print('exitcode = {}'.format(j.exitcode))


if __name__ == '__main__':
    main()
