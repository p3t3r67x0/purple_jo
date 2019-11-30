#!/usr/bin/env python3

import pyasn
import socket
import os

from datetime import datetime

from flask import request, jsonify, url_for
from flask_api import FlaskAPI, status, exceptions
from pymongo.errors import DuplicateKeyError
from flask_pymongo import PyMongo

AS_NAMES_FILE_PATH = os.path.join(os.path.dirname(__file__), 'asn_names.json')


app = FlaskAPI(__name__)

app.config['DEFAULT_RENDERERS'] = ['flask_api.renderers.JSONRenderer']
app.config['MONGO_URI'] = 'mongodb://127.0.0.1:27017/ip_data'

mongo = PyMongo(app)


def fetch_one(ipv4):
    return mongo.db.ipv4.find({'ip': ipv4}, {'_id': 0})


def fetch_latest():
    return mongo.db.ipv4.find({'as': {'$elemMatch': {'asn': {'$ne': null }}}}, {'_id': 0}).sort([('_id', -1)]).limit(50)


def asn_lookup(ipv4):
    asndb = pyasn.pyasn('rib.20191127.2000.dat', as_names_file=AS_NAMES_FILE_PATH)
    asn, prefix = asndb.lookup(ipv4)
    name = asndb.get_as_name(asn)

    return {'prefix': prefix, 'name': name, 'asn': asn, 'created': datetime.utcnow()}


@app.route('/', methods=['GET'])
def explore_data():
    return jsonify(list(fetch_latest()))


@app.route('/<string:ipv4>', methods=['GET'])
def fetch_data(ipv4):
    data = list(fetch_one(ipv4))

    if len(data) == 0:
        res = asn_lookup(ipv4)

        try:
            host = socket.gethostbyaddr(ipv4)[0]
        except Exception as e:
            host = None

        prop = {'ip': ipv4, 'host': host, 'as': [ res ]}

        try:
            mongo.db.ipv4.insert_one(prop)
        except DuplicateKeyError as e:
            pass

        if '_id' in prop:
            del prop['_id']

        data = [prop]

    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=False)
