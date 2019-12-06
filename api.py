#!/usr/bin/env python3

import re
import pyasn
import socket
import sys
import os

from datetime import datetime

from flask import jsonify
from flask_api import FlaskAPI, status
from pymongo.errors import DuplicateKeyError
from flask_pymongo import PyMongo

AS_NAMES_FILE_PATH = os.path.join(os.path.dirname(__file__), 'asn_names.json')


app = FlaskAPI(__name__, static_folder=None)

app.config['DEFAULT_RENDERERS'] = ['flask_api.renderers.JSONRenderer']
app.config['MONGO_URI'] = 'mongodb://127.0.0.1:27017/ip_data'

mongo = PyMongo(app)


def fetch_one_ip(ipv4):
    return mongo.db.lookup.find({'ip': ipv4}, {'_id': 0})


def fetch_all_prefix(prefix):
    return mongo.db.lookup.find({'cidr': {'$in': [prefix]}}, {'_id': 0}).limit(50)


def fetch_all_asn(asn):
    return mongo.db.lookup.find({'asn': int(asn)}, {'_id': 0}).limit(50)


def fetch_all_dns(domain):
    return mongo.db.dns.find({'$text': {'$search': '\'{}\''.format(domain)}},
                             {'score': {'$meta': "textScore"}, '_id': 0}).sort(
                             [('score', {'$meta': 'textScore'})]).limit(50)


def fetch_latest_dns():
    return mongo.db.dns.find({'updated': {'$exists': True},
                              'scan_failed': {'$exists': False}},
                             {'_id': 0}).sort([('updated', -1)]).limit(50)


def fetch_latest_asn():
    return mongo.db.lookup.find({'name': {'$exists': True}},
                                {'_id': 0}).sort([('updated', -1)]).limit(50)


def asn_lookup(ipv4):
    asndb = pyasn.pyasn('rib.20191127.2000.dat', as_names_file=AS_NAMES_FILE_PATH)
    asn, prefix = asndb.lookup(ipv4)
    name = asndb.get_as_name(asn)

    return {'prefix': prefix, 'name': name, 'asn': asn}


@app.route('/asn', methods=['GET'])
def explore_data():
    data = list(fetch_latest_asn())

    if data:
        return jsonify(data)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


@app.route('/asn/<string:asn>', methods=['GET'])
def fetch_data_asn(asn):
    p = re.compile(r'[a-z]', re.IGNORECASE)
    data = list(fetch_all_asn(p.sub('', asn.strip())))

    if data:
        return jsonify(data)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


@app.route('/dns', methods=['GET'])
def explore_dns():
    return jsonify(list(fetch_latest_dns()))


@app.route('/dns/<string:domain>', methods=['GET'])
def fetch_data_dns(domain):
    data = list(fetch_all_dns(domain))

    if data:
        return jsonify(data)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


@app.route('/subnet/<string:sub>/<string:prefix>', methods=['GET'])
def fetch_data_prefix(sub, prefix):
    data = list(fetch_all_prefix('{}/{}'.format(sub, prefix)))

    if data:
        return jsonify(data)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


@app.route('/ip/<string:ipv4>', methods=['GET'])
def fetch_data_ip(ipv4):
    data = list(fetch_one_ip(ipv4))

    if len(data) == 0:
        res = asn_lookup(ipv4)

        try:
            host = socket.gethostbyaddr(ipv4)[0]
        except Exception:
            host = None

        prop = {'ip': ipv4, 'host': host, 'updated': datetime.utcnow(),
                'asn': res['asn'], 'name': res['name'], 'cidr': [res['prefix']]}

        try:
            mongo.db.lookup.insert_one(prop)
        except DuplicateKeyError:
            pass

        if '_id' in prop:
            del prop['_id']

        data = [prop]

    if data:
        return jsonify(data)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


if __name__ == '__main__':
    # print(app.url_map)
    app.run(port=sys.argv[1], debug=False)
