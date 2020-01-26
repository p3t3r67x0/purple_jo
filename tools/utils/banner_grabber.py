#!/usr/bin/env python3

import socket
import multiprocessing
import argparse

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from datetime import datetime


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def update_data(db, doc_id, domain, ip, post):
    try:
        db.dns.update_one({'_id': doc_id}, {'$set': post}, upsert=False)
        print(u'INFO: updated domain {} with ip {} banner'.format(domain, ip))
    except DuplicateKeyError:
        pass


def retrieve_documents(db, skip, limit):
    return db.dns.find({'a_record': {'$exists': True},
                        'banner': {'$exists': False},
                        'ports.port': {'$in': [22]},
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


def worker(host, skip, limit):
    client = connect(host)
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

    client.close()
    return


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', help='set worker count', type=int, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


def main():
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    jobs = []
    threads = 16
    amount = db.dns.estimated_document_count() / threads
    limit = amount

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(args.host, limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        client.close()
        print('exitcode = {}'.format(j.exitcode))


if __name__ == '__main__':
    main()
