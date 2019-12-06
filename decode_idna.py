#!/usr/bin/env python3

import idna

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from idna.core import IDNAError
from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def retrieve_domains(db):
    return db.dns.find({'domain': {'$regex': '.*(xn--).*'}})


def update_data(db, id, domain, post):
    try:
        db.dns.update_one({'_id': id}, {'$set': post}, upsert=False)
        print(u'INFO: updated domain name {}'.format(domain))
    except DuplicateKeyError:
        pass


if __name__ == '__main__':
    client = connect()
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
