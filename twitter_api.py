#!/usr/bin/env python3

import os
import sys
import json
import twitter

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from datetime import datetime


def get_config():
    path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), 'config.json'))
    return ''.join(load_document(path))


def load_document(filename):
    try:
        with open(filename, 'r') as f:
            return f.readlines()
    except IOError:
        sys.exit(1)


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def connect_twitter(config):
    return twitter.Api(consumer_key=config['consumer_key'],
                       consumer_secret=config['consumer_secret'],
                       access_token_key=config['access_token_key'],
                       access_token_secret=config['access_token_secret'],
                       sleep_on_rate_limit=True)


def get_trends(api):
    return api.GetTrendsCurrent()


def get_search(api, query):
    return api.GetSearch(term='{}'.format(query), count=100, result_type='recent', return_json=True)


def add_url(db, url):
    try:
        now = datetime.utcnow()
        post = {'url': url.lower(), 'created': now}
        post_id = db.url.insert_one(post).inserted_id
        print(u'INFO: the url {} was added with the id {}'.format(url, post_id))
    except DuplicateKeyError:
        return


def main():
    client = connect()
    db = client.url_data
    db.url.create_index('url', unique=True)

    config = json.loads(get_config())
    api = connect_twitter(config)
    trends = get_trends(api)

    for trend in trends:
        tweets = get_search(api, trend.name)
        print(trend.name)

        if isinstance(tweets['statuses'], list):
            for tweet in tweets['statuses']:
                if len(tweet['entities']['urls']) > 0:
                    for url in tweet['entities']['urls']:
                        add_url(db, url['expanded_url'])


if __name__ == '__main__':
    main()
