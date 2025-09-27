#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from datetime import datetime
import re


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def add_url(db, url):
    try:
        now = datetime.now()
        post = {'url': url, 'created': now}
        post_id = db.url.insert_one(post).inserted_id

        print(f'INFO: the url {url} was added with the id {post_id}')
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
