#!/usr/bin/env python3

import argparse
import ipaddress
import multiprocessing
import pandas as pd

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound

from geoip2 import database
from geoip2.errors import AddressNotFoundError

from datetime import datetime


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_domains(db, skip, limit):
    return db.dns.find({'a_record.0': {'$exists': True},
                        'city': {'$exists': False}})[limit - skip:limit]


def update_data(db, ip, post):
    try:
        res = db.dns.update_one({'a_record': {'$in': [ip]}}, {
                                '$set': post}, upsert=False)

        if res.modified_count > 0:
            print('INFO: updated ip {} with {}'.format(ip, post['geo']))
    except DuplicateKeyError:
        pass


def read_dataframe(input):
    return pd.read_csv(input, header=None, sep=',')


def lookup_geodata(df, l):
    return df.loc[(df[0].astype(int) < l) & (df[1].astype(int) > l)]


def convert_address(ip):
    return int(ipaddress.IPv4Address(ip))


def extract_geodata(db, ip, df):
    l = convert_address(ip)
    r = lookup_geodata(df, l)

    if not r.empty:
        update_data(db, ip, {'geo': {'country_code': r[2].iloc[0],
                                     'country': r[3].iloc[0],
                                     'state': r[4].iloc[0],
                                     'city': r[5].iloc[0],
                                     'loc': {
                                         'coordinates': [r[7].iloc[0], r[6].iloc[0]]
                                     }}, 'updated': datetime.utcnow()})


def worker(host, skip, limit, df):
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    try:
        domains = retrieve_domains(db, limit, skip)

        for domain in domains:
            for ip in domain['a_record']:
                extract_geodata(db, ip, df)

        client.close()
    except CursorNotFound:
        return


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', help='set worker count',
                        type=int, required=True)
    parser.add_argument('--input', help='set the input file',
                        type=str, required=True)
    parser.add_argument('--host', help='set the host',
                        type=str, required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = argparser()
    df = read_dataframe(args.input)
    client = connect(args.host)
    db = client.ip_data

    jobs = []
    threads = args.worker
    amount = round(db.dns.estimated_document_count() / threads)
    limit = amount

    for f in range(threads):
        j = multiprocessing.Process(
            target=worker, args=(args.host, limit, amount, df))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        client.close()
        print('exitcode = {}'.format(j.exitcode))
