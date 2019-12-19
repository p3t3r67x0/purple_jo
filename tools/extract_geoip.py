#!/usr/bin/env python3

import argparse

from geoip2 import database

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from geoip2.errors import AddressNotFoundError


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_domains(db):
    return db.dns.find({'a_record.0': {'$exists': True}, 'country_code': {'$exists': False}})


def update_data(db, ip, post):
    try:
        res = db.dns.update_many({'a_record': {'$in': [ip]}}, {'$set': post}, upsert=False)

        if res.modified_count > 0:
            print('INFO: updated ip {} country code {} with {} documents'.format(ip, post['country_code'], res.modified_count))
    except DuplicateKeyError:
        pass


def extract_geodata(db, ip, input):
    reader = database.Reader(input)

    try:
        data = reader.country(ip)
    except AddressNotFoundError:
        return

    country_code = data.registered_country.iso_code

    if country_code:
        update_data(db, ip, {'country_code': country_code})


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help='set the input file', type=str, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


def main():
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    for domain in retrieve_domains(db):
        for ip in domain['a_record']:
            extract_geodata(db, ip, args.input)


if __name__ == '__main__':
    main()
