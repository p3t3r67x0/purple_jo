#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from datetime import datetime
import sys
import os
import re
import socket
import idna
import dns.resolver
import dns.exception
import dns.name
import dns.reversename
import requests
from requests.exceptions import RequestException
from requests.exceptions import ConnectionError
from requests.exceptions import Timeout
from requests.exceptions import TooManyRedirects
from requests.exceptions import InvalidURL
from requests.exceptions import MissingSchema
from urllib3.exceptions import NewConnectionError
from urllib3.exceptions import MaxRetryError


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def add_url(db, url):
    try:
        now = datetime.now()
        post = {'url': url, 'created': now}
        post_id = db.url.insert_one(post).inserted_id

        print(u'INFO: the url {} was added with the id {}'.format(url, post_id))
    except DuplicateKeyError:
        return


def load_url_file(filename):
    with open(filename, 'r') as f:
        return f.readlines()


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help='set input file name',
                        type=str, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


def main():
    args = argparser()
    client = connect(args.host)
    db = client.url_data
    db.url.create_index('url', unique=True)

    urls = load_url_file(args.input)

    for url in urls:
        url = url.strip().lower()
        url = re.sub(r'^www\.', '', url)

        print(u'INFO: the url {} is being processed'.format(url))
        add_url(db, url)
    client.close()


if __name__ == '__main__':
    main()
