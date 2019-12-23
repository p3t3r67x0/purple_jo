#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import requests
import multiprocessing
import argparse

from lxml import html
from urllib.parse import urljoin
from urllib.parse import urlparse
from fake_useragent import UserAgent
from lxml.etree import ParserError
from lxml.etree import XMLSyntaxError

from requests.exceptions import Timeout
from requests.exceptions import InvalidURL
from requests.exceptions import InvalidSchema
from requests.exceptions import MissingSchema
from requests.exceptions import ConnectionError
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ContentDecodingError
from requests.exceptions import TooManyRedirects

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import AutoReconnect

from idna.core import IDNAError
from datetime import datetime


def check_mail(url):
    return re.match(r'\b[\w.+-]+?@[-_\w]+[.]+[-_.\w]+\b', url)


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_domains(db_ip_data, skip, limit):
    return db_ip_data.dns.find({'domain_crawled': {'$exists': False}})[limit - skip:limit]


def update_data(db_ip_data, domain):
    try:
        res = db_ip_data.dns.update_one({'domain': domain}, {'$set': {'domain_crawled': datetime.utcnow()}}, upsert=False)

        if res.modified_count > 0:
            print('INFO: domain {} crawled and updated with {} documents'.format(domain, res.modified_count))
    except DuplicateKeyError:
        pass


def add_urls(db_url_data, db_ip_data, url, domain):
    try:
        post = {'url': url.lower(), 'created': datetime.utcnow()}
        post_id = db_url_data.url.insert_one(post).inserted_id

        print(u'INFO: the url {} was added with the id {}'.format(url, post_id))
        update_data(db_ip_data, domain)
    except AutoReconnect:
        time.sleep(30)
    except DuplicateKeyError as e:
        print(e)


def get_urls(db, ua, url):
    try:
        headers = {'User-Agent': ua.chrome}
        res = requests.get('http://{}'.format(url), timeout=1, headers=headers)
        content = res.text
    except (Timeout, ConnectionError, TooManyRedirects):
        return None
    except (IDNAError, InvalidURL, InvalidSchema, MissingSchema, ContentDecodingError, ChunkedEncodingError):
        return None

    try:
        doc = html.document_fromstring(content)
    except (ValueError, ParserError, XMLSyntaxError):
        return None

    links = doc.xpath('//a/@href')
    base_url = 'http://{}'.format(url)
    url_set = set()

    for link in links:
        link = link.lower().strip()

        if link.startswith('#') or link.startswith('+') or link.startswith('javascript:') or link.startswith('mailto:'):
            continue

        elif link.startswith('/'):
            link = urljoin(base_url, link)

        elif link.startswith('?'):
            link = urljoin(base_url, link)

        elif link.startswith('..'):
            link = urljoin(base_url, link.replace('..', ''))

        if len(link) > 0:
            url_set.add(link)

    print(url_set)
    return url_set


def worker(host, skip, limit):
    client = connect(host)
    db_url_data = client.url_data
    db_ip_data = client.ip_data
    ua = UserAgent()

    try:
        domains = retrieve_domains(db_ip_data, limit, skip)
    except CursorNotFound:
        client.close()
        return

    for domain in domains:
        print(u'INFO: the domain {} is beeing processed'.format(domain['domain']))
        links = get_urls(db, ua, domain['domain'])

        if links is not None and len(links) > 0:
            for link in links:
                add_urls(db_url_data, db_ip_data, link, domain['domain'])

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
    db = client.ip_data

    jobs = []
    threads = args.worker
    amount = round(db.dns.estimated_document_count() / (threads + 50000))
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
