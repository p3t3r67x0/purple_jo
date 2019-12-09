#!/usr/bin/env python3

import argparse
import ipaddress

from ipwhois.net import Net
from ipwhois.asn import ASNOrigin, IPASN
from ipwhois.exceptions import ASNOriginLookupError

from ipaddress import AddressValueError

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import DocumentTooLarge

from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def retrieve_dns(db):
    return db.dns.find({'whois': {'$exists': False},
                        'a_record.0': {'$exists': True}})


def retrieve_asns(db):
    return db.lookup.find({'whois': {'$exists': False}})


def update_data_dns(db, ip, domain, post):
    try:
        if ipaddress.IPv4Address(ip) in ipaddress.IPv4Network(post['whois']['asn_cidr']):
            data = db.dns.update_many({'a_record': {'$in': [ip]}}, {'$set': post}, upsert=False)

            if data.modified_count > 0:
                print(u'INFO: updated dns whois entry for domain {}'.format(domain))
        else:
            print('INFO: IP {} is not in subnet {}'.format(ip, post['whois']['asn_cidr']))
    except (AddressValueError, DuplicateKeyError):
        pass


def update_data_lookup(db, asn, post):
    try:
        db.lookup.update_one({'asn': asn}, {'$set': post, '$unset': {
            'subnet': 0}}, upsert=False)
        print(u'INFO: updated dns whois and cidr entry for AS{}'.format(asn))
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
        cidr = []

        cidr = ASNOrigin(Net(ip)).lookup(
            asn=str(asn), retry_count=10, asn_methods=['whois'])

        if cidr and len(cidr['nets']) > 0:
            for c in cidr['nets']:
                cidr.append(c['cidr'])

        print(cidr)
    except ASNOriginLookupError:
        cidr = None


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--collection', '-c', help='set collection to update')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = argparser()

    client = connect()
    db = client.ip_data
    now = datetime.utcnow()

    if args.collection == 'lookup':
        for asn in retrieve_asns(db):
            whois = get_whois(asn['ip'])
            cidr = get_cidr(asn['ip'], asn['asn'])

            if whois and cidr and len(whois) > 0:
                update_data_lookup(db, asn['asn'], {'updated': now, 'cidr': cidr, 'whois': whois})
    elif args.collection == 'dns':
        for dns in retrieve_dns(db):
            whois = get_whois(dns['a_record'][0])

            if whois and len(whois) > 0:
                update_data_dns(db, dns['a_record'][0], dns['domain'], {'updated': now, 'whois': whois})
