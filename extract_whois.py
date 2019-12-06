#!/usr/bin/env python3

from ipwhois.net import Net
from ipwhois.asn import ASNOrigin, IPASN
from ipwhois.exceptions import ASNOriginLookupError

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import DocumentTooLarge

from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def retrieve_asns(db):
    return db.lookup.find({'whois': {'$exists': False}})


def update_data(db, asn, post):
    try:
        db.lookup.update_one({'asn': asn}, {'$set': post, '$unset': {'subnet': 0}}, upsert=False)
        print(u'INFO: updated whois AS{} document'.format(asn))
    except (DocumentTooLarge, DuplicateKeyError):
        pass


if __name__ == '__main__':
    client = connect()
    db = client.ip_data

    for asn in retrieve_asns(db):
        now = datetime.utcnow()

        try:
            whois = IPASN(Net(asn['ip'])).lookup(retry_count=10, asn_methods=['whois'])
        except Exception:
            whois = None

        try:
            cidr_list = []

            cidr = ASNOrigin(Net(asn['ip'])).lookup(
                    asn=str(asn['asn']), retry_count=10, asn_methods=['whois'])

            if cidr and len(cidr['nets']) > 0:
                for c in cidr['nets']:
                    cidr_list.append(c['cidr'])

            print(cidr_list)
        except ASNOriginLookupError:
            cidr_list = None

        if whois and len(whois) > 0:
            update_data(db, asn['asn'], {'updated': now, 'cidr': cidr_list, 'whois': whois})
