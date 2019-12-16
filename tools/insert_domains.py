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


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def load_domains(filename):
    with open(filename, 'r') as f:
        return f.read()


def add_domains(db, domain):
    try:
        now = datetime.utcnow()
        post = {'domain': domain.lower(), 'created': now}
        post_id = db.dns.insert_one(post).inserted_id

        print(u'INFO: the domain {} was added with the id {}'.format(domain, post_id))
    except AutoReconnect:
        time.sleep(30)
    except DuplicateKeyError:
        return


def get_domains(db, ua, url):
    if check_mail(url):
        return None

    try:
        headers = {'User-Agent': ua.chrome}
        res = requests.get(u'http://{}'.format(url), timeout=1, headers=headers)
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
    url_set = set()

    for link in links:
        link = link.lower().strip()

        if link.startswith('#') or link.startswith('+') or link.startswith('javascript:') or link.startswith('mailto:'):
            continue

        elif link.startswith('/'):
            link = urljoin(url, link)

        elif link.startswith('..'):
            link = urljoin(url, link.replace('..', ''))

        if link.startswith('http'):
            try:
                domain = urlparse(link).netloc

                if len(domain) > 0:
                    url_set.add(domain)
            except ValueError:
                continue
        else:
            domain = urlparse(url).netloc

            if len(domain) > 0:
                url_set.add(domain)
            elif match_domain(url):
                url_set.add(url)

    return url_set


def check_mail(url):
    return re.match(r'\b[\w.+-]+?@[-_\w]+[.]+[-_.\w]+\b', url)


def match_all_domains(document):
    return re.findall(r'(?P<domain>[\w-]{1,63}\.?[\w\-.]{1,63}\.[\w\-\.]{2,}[\w-]?)', document)


def match_domain(domain):
    return re.match(r'(?P<domain>[\w-]{1,63}\.?[\w\-.]{1,63}\.[\w\-\.]{2,}[\w-]?)', domain)


def worker(host, domains):
    client = connect(host)
    db = client.ip_data

    for domain in domains:
        print(u'INFO: the domain {} is beeing processed'.format(domain))
        add_domains(db, domain)
        domains.remove(domain)


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help='set input file name', type=str, required=True)
    parser.add_argument('--worker', help='set worker count', type=int, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    jobs = []
    args = argparser()
    threads = args.worker
    document = load_domains(args.input)
    domains = match_all_domains(document)
    amount = round(len(domains) / threads)
    limit = amount
    print(limit, amount)

    for f in range(threads):
        j = multiprocessing.Process(target=worker, args=(args.host, (domains[limit - amount:limit]),))
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
        print('exitcode = {}'.format(j.exitcode))
