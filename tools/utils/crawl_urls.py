#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import math
import requests
import multiprocessing
import argparse

from lxml import html
from urllib.parse import urljoin, urlparse
from fake_useragent import UserAgent
from lxml.etree import ParserError, XMLSyntaxError

from requests.exceptions import (
    Timeout, InvalidURL, InvalidSchema, MissingSchema, ConnectionError,
    ChunkedEncodingError, ContentDecodingError, TooManyRedirects
)

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, AutoReconnect, WriteError, CursorNotFound
from idna.core import IDNAError
from datetime import datetime


def check_mail(url):
    return re.match(r'\b[\w.+-]+?@[-_\w]+[.]+[-_.\w]+\b', url)


def connect(host):
    # IMPORTANT: make a fresh client inside each worker, not in parent
    return MongoClient(f'mongodb://{host}:27017', connect=False)


def retrieve_domains(db_ip_data, skip, limit):
    return list(
        db_ip_data.dns.find({'domain_crawled': {'$exists': False}})
        .sort([("$natural", -1)])
        .skip(skip)
        .limit(limit)
    )


def update_data(db_ip_data, domain):
    try:
        res = db_ip_data.dns.update_one(
            {'domain': domain},
            {'$set': {'domain_crawled': datetime.utcnow()}},
            upsert=False
        )
        if res.modified_count > 0:
            print(f'INFO: domain {domain} updated')
    except DuplicateKeyError:
        pass


def add_urls(db_url_data, db_ip_data, url, domain):
    try:
        post = {'url': url.lower(), 'created': datetime.utcnow()}
        post_id = db_url_data.url.insert_one(post).inserted_id
        print(f'INFO: added url {url} with id {post_id}')
        update_data(db_ip_data, domain)
    except AutoReconnect:
        time.sleep(5)
    except (DuplicateKeyError, WriteError) as e:
        print(e)


def get_urls(ua, url):
    try:
        headers = {'User-Agent': ua.chrome}
        res = requests.get(f'http://{url}', timeout=2, headers=headers)
        content = res.text
    except (Timeout, ConnectionError, TooManyRedirects,
            IDNAError, InvalidURL, InvalidSchema, MissingSchema,
            ContentDecodingError, ChunkedEncodingError):
        return None

    try:
        doc = html.document_fromstring(content)
    except (ValueError, ParserError, XMLSyntaxError):
        return None

    links = doc.xpath('//a/@href')
    base_url = f'http://{url}'
    url_set = set()

    for link in links:
        link = link.lower().strip()
        if link.startswith(('#', '+', 'tel:', 'javascript:', 'mailto:')):
            continue
        elif link.startswith(('/', '?', '..')):
            link = urljoin(base_url, link)

        if urlparse(link).netloc:
            url_set.add(link)

    return url_set


def worker(task):
    host, skip, limit = task
    client = connect(host)
    db_url_data = client.url_data
    db_ip_data = client.ip_data
    ua = UserAgent()

    try:
        domains = retrieve_domains(db_ip_data, skip, limit)
    except CursorNotFound:
        client.close()
        return f"Worker {skip}:{skip+limit} cursor error"

    for domain in domains:
        d = domain['domain']
        print(f'INFO: processing domain {d}')
        links = get_urls(ua, d)

        if links:
            for link in links:
                add_urls(db_url_data, db_ip_data, link, d)

    client.close()
    return f"Worker {skip}:{skip+limit} done ({len(domains)} domains)"


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', type=int, required=True,
                        help='set worker count')
    parser.add_argument('--host', type=str, required=True,
                        help='set Mongo host')
    return parser.parse_args()


if __name__ == '__main__':
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    total_docs = db.dns.count_documents({'domain_crawled': {'$exists': False}})
    client.close()

    chunk_size = math.ceil(total_docs / args.worker)
    tasks = [(args.host, i * chunk_size, chunk_size)
             for i in range(args.worker)]

    with multiprocessing.Pool(processes=args.worker) as pool:
        for result in pool.imap_unordered(worker, tasks):
            print(result)
