#!/usr/bin/env python3

from ipwhois.net import Net
from ipwhois.asn import ASNOrigin
from ipwhois.exceptions import ASNOriginLookupError

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def retrieve_asns(db):
    return db.lookup.find({})


def update_data(db, asn, post):
    try:
        db.asn.update_one({'asn': asn}, {'$set': post}, upsert=False)
        print(u'INFO: updated whois AS{} document'.format(asn))
    except DuplicateKeyError:
        pass


if __name__ == '__main__':
    client = connect()
    db = client.ip_data

    for asn in retrieve_asns(db):
        now = datetime.utcnow()

        try:
            whois = ASNOrigin(net=Net(asn['ip'])).lookup(
                    asn=str(asn['asn']), retry_count=10, asn_methods=['whois'])
        except ASNOriginLookupError:
            whois = None

        if whois and len(whois['nets']) > 0:
            update_data(db, str(asn['asn']), {'updated': now, 'whois': whois['nets']})
