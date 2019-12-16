#!/usr/bin/env python3

from geoip2 import database

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from geoip2.errors import AddressNotFoundError


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def retrieve_domains(db):
    return db.dns.find({'a_record.0': {'$exists': True}, 'country_code': {'$exists': False}})


def update_data(db, ip, post):
    try:
        res = db.dns.update_many({'a_record': {'$in': [ip]}}, {'$set': post}, upsert=False)

        if res.modified_count > 0:
            print('INFO: updated ip {} country code {} with {} documents'.format(ip, post['country_code'], res.modified_count))
    except DuplicateKeyError:
        pass


def extract_geodata(db, ip):
    reader = database.Reader('GeoLite2-Country.mmdb')

    try:
        data = reader.country(ip)
    except AddressNotFoundError:
        return

    country_code = data.registered_country.iso_code

    if country_code:
        update_data(db, ip, {'country_code': country_code})


def main():
    client = connect()
    db = client.ip_data

    for domain in retrieve_domains(db):
        for ip in domain['a_record']:
            extract_geodata(db, ip)


if __name__ == '__main__':
    main()
