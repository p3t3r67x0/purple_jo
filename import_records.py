#!/usr/bin/env python3

import sys
import json
import argparse
import multiprocessing
import time

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import AutoReconnect

from json.decoder import JSONDecodeError
from datetime import datetime


def load_document(filename):
    try:
        with open(filename, 'r') as f:
            return f.readlines()
    except IOError:
        sys.exit(1)


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def update_data(db, domain, record_type, now, record):
    try:
        # print({'domain': domain}, {'$set': {'updated': now}, '$addToSet': {record_type: record}})
        res = db.dns.update_one({'domain': domain, record_type:
                                 {'$not': {'$in': [record]}}},
                                 {'$set': {'updated': now}, '$addToSet':
                                 {record_type: record}}, upsert=False)
        if res.modified_count > 0:
            print('INFO: updated {} document type {} for domain {}'.format(res.modified_count, record_type, domain))
    except AutoReconnect:
        time.sleep(30)
    except DuplicateKeyError:
        pass


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', help='set input file name')
    args = parser.parse_args()

    return args


def worker(records):
    client = connect()
    db = client.ip_data

    for record in records:
        records.remove(record)

        try:
            r = json.loads(record.strip())
        except JSONDecodeError:
            return

        domain = r['query_name'].lower().strip('.')

        if r['resp_type'] == 'A':
            update_data(db, domain, 'a_record', datetime.utcnow(), r['data'].lower().strip('.'))

        if r['resp_type'] == 'AAAA':
            update_data(db, domain, 'aaaa_record', datetime.utcnow(), r['data'].lower().strip('.'))

        if r['resp_type'] == 'CNAME':
            cname_record = {'target': r['data'].lower().strip('.')}
            update_data(db, domain, 'cname_record', datetime.utcnow(), cname_record)

        if r['resp_type'] == 'NS':
            update_data(db, domain, 'ns_record', datetime.utcnow(), r['data'].lower().strip('.'))

        if r['resp_type'] == 'MX':
            mx_record = {'preference': r['data'].split(' ')[0], 'exchange': r['data'].split(' ')[1].lower().strip('.')}
            update_data(db, domain, 'mx_record', datetime.utcnow(), mx_record)

        if r['resp_type'] == 'SOA':
            update_data(db, domain, 'soa_record', datetime.utcnow(), r['data'].lower().strip('.'))

    return


if __name__ == '__main__':
    jobs = []
    threads = 64
    args = argparser()
    records = load_document(args.input)
    amount = round(len(records) / threads)
    limit = amount
    print(limit, amount)

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=((records[limit - amount:limit]),))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        print('exitcode = {}'.format(j.exitcode))
