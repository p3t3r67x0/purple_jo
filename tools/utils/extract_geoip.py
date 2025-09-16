#!/usr/bin/env python3

import argparse
import multiprocessing

from geoip2 import database

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import CursorNotFound

from geoip2.errors import AddressNotFoundError


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_domains(db, skip, limit):
    return db.dns.find({'a_record.0': {'$exists': True},
                        'country_code': {'$exists': False}})[limit - skip:limit]


def update_data(db, ip, post):
    try:
        res = db.dns.update_one({'a_record': {'$in': [ip]}}, {
                                '$set': post}, upsert=False)

        if res.modified_count > 0:
            print('INFO: updated ip {} country code {} with {} documents'.format(
                ip, post['country_code'], res.modified_count))
    except DuplicateKeyError:
        pass


def extract_geodata(db, ip, input):
    reader = database.Reader(input)

    try:
        response = reader.city(ip)
        print(response)
        country_code = response.country.iso_code
        country = response.country.name
        state = response.subdivisions.most_specific.name
        city = response.city.name
        latitude = response.location.latitude
        longitude = response.location.longitude
        
        if latitude is not None or longitude is not None:
            longitude = round(longitude, 5)
            latitude = round(latitude, 5)

        update_data(db, ip, {'country_code': country_code,
                             'country': country,
                             'state': state,
                             'city': city,
                             'loc': {'coordinates': [longitude, latitude]}})
    except AddressNotFoundError:
        print("IP not found in database")
    finally:
        reader.close()


def worker(host, skip, limit):
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    try:
        domains = retrieve_domains(db, limit, skip)

        for domain in domains:
            for ip in domain['a_record']:
                extract_geodata(db, ip, args.input)

        client.close()
    except CursorNotFound:
        return


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', help='set worker count',
                        type=int, required=True)
    parser.add_argument('--input', help='set the input file',
                        type=str, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    jobs = []
    threads = args.worker
    amount = round(db.dns.estimated_document_count() / threads)
    limit = amount

    for f in range(threads):
        j = multiprocessing.Process(
            target=worker, args=(args.host, limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        client.close()
        print('exitcode = {}'.format(j.exitcode))
