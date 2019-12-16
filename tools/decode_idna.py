#!/usr/bin/env python3

import idna
import argparse

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from idna.core import IDNAError
from datetime import datetime


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_domains(db):
    return db.dns.find({'domain': {'$regex': '.*(xn--).*'}})


def update_data(db, id, domain, post):
    try:
        db.dns.update_one({'_id': id}, {'$set': post}, upsert=False)
        print(u'INFO: updated domain name {}'.format(domain))
    except DuplicateKeyError:
        pass


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    domains = retrieve_domains(db)

    for domain in domains:
        now = datetime.utcnow()
        idna_domain = domain['domain']

        try:
            decoded_domain = idna.decode(idna_domain)
        except (UnicodeError, IDNAError):
            decoded_domain = None

        if decoded_domain and idna_domain != decoded_domain:
            update_data(db, domain['_id'], decoded_domain, {'updated': now, 'domain': decoded_domain})
