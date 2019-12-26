#!/usr/bin/env python3

import argparse
import ipaddress
import multiprocessing
import argparse

from ipwhois.net import Net
from ipwhois.asn import ASNOrigin, IPASN
from ipwhois.exceptions import ASNOriginLookupError

from ipaddress import AddressValueError

from pymongo import MongoClient
from pymongo.errors import CursorNotFound
from pymongo.errors import DuplicateKeyError
from pymongo.errors import DocumentTooLarge

from datetime import datetime


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_dns(db, limit, skip):
    return db.dns.find({'whois.asn': {'$exists': False},
                        'a_record.0': {'$exists': True}
                        }).sort([('updated', -1)])[limit - skip:limit]


def retrieve_asns(db, limit, skip):
    return db.lookup.find({'whois.asn': {'$exists': False}})[limit - skip:limit]


def update_data_dns(db, ip, domain, post):
    try:
        if ipaddress.IPv4Address(ip) in ipaddress.IPv4Network(post['whois']['asn_cidr']):
            res = db.dns.update_many({'a_record': {'$in': [ip]},
                                      'whois.asn': {'$exists': False}}, {
                                      '$set': post}, upsert=False)

            if res.modified_count > 0:
                print(u'INFO: updated {} whois entries for domain {}'.format(res.modified_count, domain))
            else:
                print(u'INFO: nothing to update for domain {}'.format(domain))
        else:
            print('INFO: IP {} is not in subnet {}'.format(ip, post['whois']['asn_cidr']))
    except (AddressValueError, DuplicateKeyError):
        pass


def update_data_lookup(db, asn, post):
    try:
        res = db.lookup.update_many({'asn': asn}, {'$set': post, '$unset': {
                                     'subnet': 0}}, upsert=False)
        print(u'INFO: updated dns whois and cidr entry for AS{}, {} document modified'.format(asn, res.modified_count))
    except (DocumentTooLarge, DuplicateKeyError):
        pass


def get_whois(ip):
    try:
        whois = IPASN(Net(ip)).lookup(retry_count=10, asn_methods=['whois'])
    except Exception:
        whois = None

    return whois


def get_cidr(ip, asn):
    try:
        x = []

        cidr = ASNOrigin(Net(ip)).lookup(
            asn=str(asn), retry_count=10, asn_methods=['whois'])

        if cidr and len(cidr['nets']) > 0:
            for c in cidr['nets']:
                x.append(c['cidr'])

        return x
    except ASNOriginLookupError:
        return None


def worker(host, limit, skip, col):
    client = connect(host)
    db = client.ip_data
    now = datetime.utcnow()

    if col == 'lookup':
        try:
            for asn in retrieve_asns(db, limit, skip):
                whois = get_whois(asn['ip'])
                cidr = get_cidr(asn['ip'], asn['asn'])

                if whois and cidr and len(whois) > 0:
                    update_data_lookup(db, asn['asn'], {'updated': now, 'cidr': cidr, 'whois': whois})
        except CursorNotFound:
            pass

    elif col == 'dns':
        try:
            for dns in retrieve_dns(db, limit, skip):
                whois = get_whois(dns['a_record'][0])

                if whois and len(whois) > 0:
                    update_data_dns(db, dns['a_record'][0], dns['domain'], {'updated': now, 'whois': whois})
        except CursorNotFound:
            pass

    client.close()
    return


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--collection', help='set collection to update', type=str, required=True)
    parser.add_argument('--worker', help='set worker count', type=int, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    jobs = []
    threads = args.worker
    amount = round(db[args.collection].estimated_document_count() / (threads + 50000))
    limit = amount
    print(limit, amount)

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(args.host, limit, amount, args.collection))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        client.close()
        print('exitcode = {}'.format(j.exitcode))
