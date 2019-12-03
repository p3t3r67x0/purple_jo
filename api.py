#!/usr/bin/env python3

import pyasn
import socket
import sys
import os

from datetime import datetime

from flask import jsonify
from flask_api import FlaskAPI
from pymongo.errors import DuplicateKeyError
from flask_pymongo import PyMongo

AS_NAMES_FILE_PATH = os.path.join(os.path.dirname(__file__), 'asn_names.json')


app = FlaskAPI(__name__)

app.config['DEFAULT_RENDERERS'] = ['flask_api.renderers.JSONRenderer']
app.config['MONGO_URI'] = 'mongodb://127.0.0.1:27017/ip_data'

mongo = PyMongo(app)


def fetch_one(ipv4):
    return mongo.db.ipv4.find({'ip': ipv4}, {'_id': 0})


def fetch_one_prefix(prefix):
    return mongo.db.ipv4.find({'as.prefix': prefix}, {'_id': 0})


def fetch_one_dns(domain):
    return mongo.db.dns.find({'domain': domain}, {'_id': 0})


def fetch_latest_dns():
    return mongo.db.dns.find({'updated': {'$exists': True},
                              'header.status': {'$exists': True},
                              'scan_failed': {'$exists': False}},
                             {'_id': 0}).sort([('updated', -1)]).limit(50)


def fetch_latest():
    return mongo.db.ipv4.find({'as': {'$elemMatch': {'asn': {'$ne': None}}}},
                              {'_id': 0}).sort([('as.created', -1)]).limit(50)


def asn_lookup(ipv4):
    asndb = pyasn.pyasn('rib.20191127.2000.dat', as_names_file=AS_NAMES_FILE_PATH)
    asn, prefix = asndb.lookup(ipv4)
    name = asndb.get_as_name(asn)

    return {'prefix': prefix, 'name': name, 'asn': asn, 'created': datetime.utcnow()}


@app.route('/ip', methods=['GET'])
def explore_data():
    return jsonify(list(fetch_latest()))


@app.route('/dns', methods=['GET'])
def explore_dns():
    return jsonify(list(fetch_latest_dns()))


@app.route('/dns/<string:domain>', methods=['GET'])
def fetch_data_dns(domain):
    data = list(fetch_one_dns(domain))
    return jsonify(data)


@app.route('/subnet/<string:sub>/<string:prefix>', methods=['GET'])
def fetch_data_prefix(sub, prefix):
    data = list(fetch_one_prefix('{}/{}'.format(sub, prefix)))
    return jsonify(data)


@app.route('/ip/<string:ipv4>', methods=['GET'])
def fetch_data(ipv4):
    data = list(fetch_one(ipv4))

    if len(data) == 0:
        res = asn_lookup(ipv4)

        try:
            host = socket.gethostbyaddr(ipv4)[0]
        except Exception:
            host = None

        prop = {'ip': ipv4, 'host': host, 'as': [res]}

        try:
            mongo.db.ipv4.insert_one(prop)
        except DuplicateKeyError:
            pass

        if '_id' in prop:
            del prop['_id']

        data = [prop]

    return jsonify(data)


if __name__ == '__main__':
    app.run(port=sys.argv[1], debug=False)
