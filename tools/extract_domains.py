#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import multiprocessing
import argparse

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import AutoReconnect

from urllib.parse import urlparse
from datetime import datetime


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_urls(db_url_data, skip, limit):
    return db_url_data.url.find({'domain_extracted': {'$exists': False}})[limit - skip:limit]


def add_domains(db_url_data, db_ip_data, url_id, domain):
    update_data(db_url_data, url_id)

    try:
        post = {'domain': domain.lower(), 'created': datetime.utcnow()}
        post_id = db_ip_data.dns.insert_one(post).inserted_id

        print(u'INFO: the domain {} was added with the id {}'.format(domain, post_id))
    except AutoReconnect:
        time.sleep(30)
    except DuplicateKeyError as e:
        print(e)
        return


def update_data(db_url_data, url_id):
    db_url_data.url.update_one({'_id': url_id}, {'$set': {'domain_extracted': datetime.utcnow()}}, upsert=False)


def worker(host, skip, limit):
    client = connect(host)
    db_url_data = client.url_data
    db_ip_data = client.ip_data

    try:
        urls = retrieve_urls(db_url_data, limit, skip)
    except CursorNotFound:
        client.close()
        return

    for url in urls:
        domain = urlparse(url['url']).netloc
        print(u'INFO: the url {} is beeing processed'.format(url['url']))
        print(domain)
        add_domains(db_url_data, db_ip_data, url['_id'], domain)

    client.close()
    return


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', help='set worker count', type=int, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = argparser()
    client = connect(args.host)
    db_url_data = client.url_data

    jobs = []
    threads = args.worker
    amount = round(db_url_data.url.estimated_document_count() / threads)
    limit = amount
    print(limit, amount)

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(args.host, limit, amount))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        client.close()
        j.join()
        print('exitcode = {}'.format(j.exitcode))
