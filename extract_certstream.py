#!/usr/bin/env python3

import logging
import certstream

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from datetime import datetime


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def add_domain(db, domain):
    try:
        db.dns.insert_one({'domain': domain, 'created': datetime.utcnow()})
    except DuplicateKeyError as e:
        print(e)


def print_callback(message, context):
    logging.debug("Message -> {}".format(message))

    client = connect()
    db = client.ip_data
    db.dns.create_index('domain', unique=True)

    if message['message_type'] == "heartbeat":
        return

    if message['message_type'] == "certificate_update":
        all_domains = message['data']['leaf_cert']['all_domains']

        if len(all_domains) == 0:
            domain = "NULL"
        else:
            domain = all_domains[0]

        domain = domain.replace('*.', '')
        add_domain(db, domain)
        print(domain)

        for domain in message['data']['leaf_cert']['all_domains'][1:]:
            domain = domain.replace('*.', '')
            add_domain(db, domain)
            print(domain)

    client.close()


def main():
    logging.basicConfig(
        format='[%(levelname)s:%(name)s] %(asctime)s - %(message)s',
        level=logging.INFO)

    certstream.listen_for_events(
        print_callback, url='wss://certstream.calidog.io')


if __name__ == '__main__':
    main()
