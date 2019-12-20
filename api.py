#!/usr/bin/env python3

import re
import pyasn
import socket
import argparse
import sys
import os

from datetime import datetime

from flask import jsonify
from flask_api import FlaskAPI, status
from pymongo.errors import DuplicateKeyError
from werkzeug.routing import PathConverter
from flask_pymongo import PyMongo

AS_NAMES_FILE_PATH = os.path.join(os.path.dirname(__file__), 'asn_names.json')


app = FlaskAPI(__name__, static_folder=None)

app.config.from_pyfile('config.cfg')

mongo = PyMongo(app)


class EverythingConverter(PathConverter):
    regex = '.*?'


app.url_map.converters['match'] = EverythingConverter


def fetch_one_ip(ip):
    return mongo.db.dns.find({'a_record': {'$in': [ip]}}, {'_id': 0})


def fetch_match_condition(condition, query):
    if query is not None:
        if condition == 'registry':
            return mongo.db.dns.find({'whois': {'$exists': True},
                                      'whois.asn_registry': query}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'port':
            return mongo.db.dns.find({'ports': {'$exists': True},
                                      'ports.port': int(query)}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'status':
            return mongo.db.dns.find({'header': {'$exists': True},
                                      'header.status': query}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'ssl':
            return mongo.db.dns.find({'ssl_cert.subject': {'$exists': True},
                                      'ssl_cert.subject.common_name': {
                                      '$regex': query.lower()}}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'app':
            return mongo.db.dns.find({'header': {'$exists': True}, 'header.x-powered-by': {
                                      '$regex': query.lower(), '$options': 'i'}}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'country':
            return mongo.db.dns.find({'whois': {'$exists': True},
                                      'whois.asn_country_code': query.upper()}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'banner':
            return mongo.db.dns.find({'banner': {'$regex': query.lower(), '$options': 'i'}}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'asn':
            p = re.compile(r'[a-z:]', re.IGNORECASE)
            return mongo.db.dns.find({'whois': {'$exists': True}, 'whois.asn': p.sub('', query.lower())}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'org':
            return mongo.db.dns.find({'whois': {'$exists': True}, 'whois.asn_description': {
                                      '$regex': query.lower(), '$options': 'i'}}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'cidr':
            return mongo.db.dns.find({'whois': {'$exists': True},
                                      'whois.asn_cidr': query}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'cname':
            return mongo.db.dns.find({'cname_record': {'$exists': True},
                                      'cname_record.target': {'$in': [query.lower()]}}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'mx':
            return mongo.db.dns.find({'mx_record': {'$exists': True},
                                      'mx_record.exchange': {'$in': [query.lower()]}}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'server':
            return mongo.db.dns.find({'header': {'$exists': True}, 'header.server': {
                                      '$regex': query.lower(), '$options': 'i'}}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)
        elif condition == 'site':
            return mongo.db.dns.find({'domain': query.lower()}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)

        elif condition == 'ipv4':
            return mongo.db.dns.find({'a_record': {'$in': [query.lower()]}}, {
                                      '_id': 0}).sort([('updated', -1)]).limit(30)


def fetch_all_prefix(prefix):
    return mongo.db.lookup.find({'cidr': {'$in': [prefix]}}, {'_id': 0}).sort([('updated', -1)]).limit(30)


def fetch_all_asn(asn):
    return mongo.db.lookup.find({'whois.asn': asn}, {'_id': 0}).limit(5)


def fetch_all_dns(domain):
    return mongo.db.dns.find({'$text': {'$search': '\'{}\''.format(domain)}},
                             {'score': {'$meta': "textScore"}, '_id': 0}).sort(
                             [('score', {'$meta': 'textScore'})]).limit(30)


def fetch_latest_dns():
    return mongo.db.dns.find({'updated': {'$exists': True},
                              'scan_failed': {'$exists': False}},
                              {'_id': 0}).sort([('updated', -1)]).limit(30)


def fetch_latest_cidr():
    return mongo.db.dns.find({'whois.asn_cidr': {'$exists': True}}, {'_id': 0,
                              'whois.asn': 1, 'whois.asn_cidr': 1}).limit(200)


def fetch_latest_ipv4():
    return mongo.db.dns.find({'a_record': {'$exists': True}}, {'_id': 0,
                              'a_record': 1, 'country_code': 1}).sort(
                              [('updated', -1)]).limit(200)


def fetch_latest_asn():
    return mongo.db.dns.find({'whois.asn': {'$exists': True}}, {'_id': 0,
                              'whois.asn': 1, 'whois.asn_country_code': 1}).limit(200)


def asn_lookup(ipv4):
    asndb = pyasn.pyasn('rib.20191127.2000.dat', as_names_file=AS_NAMES_FILE_PATH)
    asn, prefix = asndb.lookup(ipv4)
    name = asndb.get_as_name(asn)

    return {'prefix': prefix, 'name': name, 'asn': asn}


@app.route('/dns/', methods=['GET'])
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


@app.route('/match/<path:query>', methods=['GET'])
def fetch_data_condition(query):
    q = re.sub(r'[\'"(){}]', '', query).split(':')
    data = list(fetch_match_condition(q[0], q[1]))

    if data:
        return jsonify(data)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


@app.route('/asn', methods=['GET'])
def fetch_data_asn():
    data = list(fetch_latest_asn())

    if data:
        return jsonify(data)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


@app.route('/cidr', methods=['GET'])
def fetch_data_cidr():
    data = list(fetch_latest_cidr())

    if data:
        return jsonify(data)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


@app.route('/ipv4', methods=['GET'])
def fetch_data_ipv4():
    data = list(fetch_latest_ipv4())
    v = []

    for d in data:
        if 'country_code' in d:
            c = d['country_code']
        else:
            c = None

        for r in d['a_record']:
            v.append({'a_record': r, 'country_code': c})

    if v:
        return jsonify(v)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


@app.route('/', methods=['GET'])
def fetch_nothing():
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


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', help='set the boolean for debug', type=bool, default=False)
    parser.add_argument('--port', help='set the port', type=int, required=True)
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = argparser()
    app.run(port=args.port, debug=args.debug)
