#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import math
import multiprocessing
import argparse
from datetime import datetime
from urllib.parse import urlparse

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, AutoReconnect, CursorNotFound


def connect(host):
    # Create a fresh connection in each worker
    return MongoClient(f'mongodb://{host}:27017', connect=False)


def match_ipv4(ipv4):
    return re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', ipv4)


def find_domain(domain):
    return re.search(
        r'([\w\-.]{1,63}|[\w\-.]{1,63}[^\x00-\x7F\w-]{1,63})\.([\w\-.]{2,})|'
        r'(([\w\d-]{1,63}|[\d\w-]*[^\x00-\x7F\w-]{1,63}))\.?'
        r'([\w\d]{1,63}|[\d\w\-.]*[^\x00-\x7F\-.]{1,63})\.'
        r'([a-z\.]{2,}|[\w]*[^\x00-\x7F\.]{2,})',
        domain
    )


def retrieve_urls(db_url_data, skip, limit):
    return list(
        db_url_data.url.find({'domain_extracted': {'$exists': False}})
        .sort([("$natural", -1)])
        .skip(skip)
        .limit(limit)
    )


def update_data(db_url_data, url_id):
    db_url_data.url.update_one(
        {'_id': url_id},
        {'$set': {'domain_extracted': datetime.utcnow()}},
        upsert=False
    )


def add_domains(db_url_data, db_ip_data, url_id, domain):
    update_data(db_url_data, url_id)
    try:
        post = {'domain': domain.lower(), 'created': datetime.utcnow()}
        post_id = db_ip_data.dns.insert_one(post).inserted_id
        print(f"INFO: added domain {domain} with id {post_id}")
    except AutoReconnect:
        time.sleep(5)
    except DuplicateKeyError as e:
        print(f"Duplicate domain {domain}: {e}")


def worker(task):
    host, skip, limit = task
    client = connect(host)
    db_url_data = client.url_data
    db_ip_data = client.ip_data

    try:
        urls = retrieve_urls(db_url_data, skip, limit)
        for url in urls:
            try:
                domain = find_domain(url['url'])
                if domain is not None and not match_ipv4(domain.group(0)):
                    print(f"INFO: processing {url['url']}")
                    add_domains(db_url_data, db_ip_data,
                                url['_id'], domain.group(0))
            except ValueError:
                continue
    except CursorNotFound:
        print(f"Cursor error for batch {skip}:{skip+limit}")
    finally:
        client.close()
    return f"Worker {skip}:{skip+limit} done"


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', type=int, required=True,
                        help='number of workers')
    parser.add_argument('--host', type=str, required=True, help='MongoDB host')
    return parser.parse_args()


if __name__ == '__main__':
    args = argparser()
    client = connect(args.host)
    total_docs = client.url_data.url.count_documents(
        {'domain_extracted': {'$exists': False}})
    client.close()

    chunk_size = math.ceil(total_docs / args.worker)
    tasks = [(args.host, i * chunk_size, chunk_size)
             for i in range(args.worker)]

    with multiprocessing.Pool(processes=args.worker) as pool:
        for result in pool.imap_unordered(worker, tasks):
            print(result)
