#!/usr/bin/env python3

import re
import time
import multiprocessing
import argparse

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from utils.extract_geodata import read_dataframe
from utils.update_entry import handle_query


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_mx_records(db, skip, limit):
    return db.dns.find({'mx_record.exchange': {'$exists': True}},
                       {'_id': 0, 'mx_record.exchange': 1})[limit - skip:limit]


def retrieve_domain(db, domain):
    return db.dns.find_one({'domain': domain})


def worker(df, host, skip, limit):
    client = connect(host)
    db = client.ip_data
    mx_records_uniq = set()

    mx_records = retrieve_mx_records(db, limit, skip)

    for mx_record in mx_records:
        for mx in mx_record['mx_record']:
            mx_records_uniq.add(mx['exchange'])

    for mx_record in mx_records_uniq:
        data = retrieve_domain(db, mx_record)

        if not data:
            handle_query(mx_record, df)


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', help='set worker count', type=int, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = argparser()
    df = read_dataframe('data/geodata.csv')
    client = connect(args.host)
    db = client.ip_data

    jobs = []
    threads = args.worker
    amount = round(db.dns.estimated_document_count() / (threads + 5000))
    limit = amount
    print(limit, amount)

    for f in range(threads):
        j = multiprocessing.Process(
            target=worker, args=(df, args.host, limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        client.close()
        j.join()
        print('exitcode = {}'.format(j.exitcode))
