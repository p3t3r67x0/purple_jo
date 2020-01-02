#!/usr/bin/env python3

import re
import pyasn
import socket
import argparse
import json
import uuid
import sys
import os

from datetime import datetime

from flask import jsonify
from flask_api import FlaskAPI, status
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError
from werkzeug.exceptions import NotFound, BadRequest, BadGateway, MethodNotAllowed, RequestEntityTooLarge, InternalServerError
from werkzeug.routing import PathConverter
from logging.config import dictConfig
from flask_pymongo import PyMongo
from rejson import Client, Path
from bson import json_util


dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})


app = FlaskAPI(__name__, static_folder=None)

app.config.from_pyfile('config.cfg')

mongo = PyMongo(app)


class EverythingConverter(PathConverter):
    regex = '.*?'


app.url_map.converters['match'] = EverythingConverter


@app.errorhandler(TypeError)
def handle_not_found(e):
    app.logger.error('type: {}, args: {}'.format(type(e).__name__, e.args))
    return jsonify(message='Something went wrong application error'), 500


@app.errorhandler(NotFound)
def handle_not_found(e):
    app.logger.error('type: {}, args: {}'.format(type(e).__name__, e.args))
    return jsonify(message='Requested resource was not found on server'), 404


@app.errorhandler(BadRequest)
def handle_bad_request(e):
    app.logger.error('type: {}, args: {}'.format(type(e).__name__, e.args))
    return jsonify(message='Bad request, the error has been reported'), 400


@app.errorhandler(BadGateway)
def handle_bad_request(e):
    app.logger.error('type: {}, args: {}'.format(type(e).__name__, e.args))
    return jsonify(message='Application programm interface is not reachable'), 502


@app.errorhandler(MethodNotAllowed)
def handle_method_not_allowed(e):
    app.logger.error('type: {}, args: {}'.format(type(e).__name__, e.args))
    return jsonify(message='The method is not allowed for the requested URL'), 405


@app.errorhandler(InternalServerError)
def handle_internal_server_error(e):
    app.logger.error('type: {}, args: {}'.format(type(e).__name__, e.args))
    return jsonify(message='Something went wrong, internal server error'), 500


@app.errorhandler(RequestEntityTooLarge)
def handle_request_entity_too_large(e):
    app.logger.error('type: {}, args: {}'.format(type(e).__name__, e.args))
    return jsonify(message='The file transmitted exceeds the capacity limit'), 413


@app.errorhandler(ServerSelectionTimeoutError)
def handle_not_found(e):
    app.logger.error('type: {}, args: {}'.format(type(e).__name__, e.args))
    return jsonify(message='Something went wrong application error'), 500


def connect_cache():
    return Client(host='127.0.0.1', port=6379, decode_responses=True)


def fetch_one_ip(ip):
    query = {'a_record': {'$in': [ip]}}
    context = {'_id': 0}


def fetch_from_cache(query, context, sort, limit, cache_key):
    stored = cache.smembers(cache_key)
    cache_list = []

    if len(stored) == 0:
        store_cache(query, context, sort, limit, cache_key)
        return list(mongo.db.dns.find(query, context).sort([sort]).limit(limit))

    for store in stored:
        cache_list.append(cache.jsonget(store, Path.rootPath()))

    return cache_list


def store_cache(query, context, sort, limit, cache_key, reset=False):
    if reset:
        cache.delete(cache_key)

    docs = mongo.db.dns.find(query, context).sort([sort]).limit(limit)

    for doc in docs:
        uid = hash(uuid.uuid4())
        cache.jsonset(uid, Path.rootPath(), json.loads(
            json.dumps(doc, default=json_util.default)))
        cache.sadd(cache_key, uid)


def create_index(field_name_1, field_name_2):
    mongo.db.dns.create_index(
        [(field_name_1, DESCENDING), (field_name_2, DESCENDING)], background=True)


def fetch_match_condition(condition, query):
    if query is not None:
        if condition == 'registry':
            sub_query = query.lower()

            query = {'whois.asn_registry': sub_query}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'registry-{}'.format(sub_query))
        elif condition == 'port':
            sub_query = int(query)

            query = {'ports.port': sub_query}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'port-{}'.format(sub_query))
        elif condition == 'status':
            query = {'header.status': query}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'status-{}'.format(query))
        elif condition == 'ssl':
            sub_query = query.lower()

            query = {'ssl_cert.subject.common_name': {'$regex': sub_query}}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'ssl-{}'.format(sub_query))
        elif condition == 'app':
            sub_query = query.lower()

            query = {'header.x-powered-by': {'$regex': sub_query, '$options': 'i'}}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'app-{}'.format(sub_query))
        elif condition == 'country':
            sub_query = query.upper()

            query = {'whois.asn_country_code': sub_query}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'country-{}'.format(sub_query))
        elif condition == 'banner':
            sub_query = query.lower()

            query = {'banner': {'$regex': sub_query, '$options': 'i'}}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'banner-{}'.format(sub_query))
        elif condition == 'asn':
            sub_query = re.sub(r'[a-zA-Z:]', '', query.lower())

            query = {'whois.asn': sub_query}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'asn-{}'.format(sub_query))
        elif condition == 'org':
            sub_query = query.lower()

            query = {'whois.asn_description': {
                     '$regex': sub_query, '$options': 'i'}}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'org-{}'.format(sub_query))
        elif condition == 'cidr':
            sub_query = query.lower()

            query = {'whois.asn_cidr': query}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'cidr-{}'.format(sub_query))
        elif condition == 'cname':
            sub_query = query.lower()

            query = {'cname_record.target': {'$in': [query.lower()]}}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'cname-{}'.format(sub_query))
        elif condition == 'mx':
            sub_query = query.lower()

            query = {'mx_record.exchange': {'$in': [query.lower()]}}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'mx-{}'.format(sub_query))
        elif condition == 'server':
            sub_query = query.lower()

            query = {'header.server': {'$regex': sub_query, '$options': 'i'}}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'server-{}'.format(sub_query))
        elif condition == 'site':
            sub_query = query.lower()

            query = {'domain': sub_query}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'site-{}'.format(sub_query))
        elif condition == 'ipv4':
            sub_query = query.lower()

            query = {'a_record': {'$in': [sub_query]}}
            context = {'_id': 0}
            sort = ('updated', -1)
            limit = 30

            return fetch_from_cache(query, context, sort, limit, 'ipv4-{}'.format(sub_query))


def fetch_all_prefix(prefix):
    return mongo.db.lookup.find({'cidr': {'$in': [prefix]}}, {'_id': 0})


def fetch_all_asn(asn):
    return mongo.db.lookup.find({'whois.asn': asn}, {'_id': 0}).limit(5)


def fetch_query_dns(domain):
    query = {'$text': {'$search': '"{}"'.format(domain)}}
    context = {'score': {'$meta': "textScore"}, '_id': 0}
    sort = ('score', {'$meta': 'textScore'})
    limit = 200

    return fetch_from_cache(query, context, sort, limit, domain)


def fetch_latest_dns():
    query = {'updated': {'$exists': True}, 'scan_failed': {'$exists': False}}
    context = {'_id': 0}
    sort = ('updated', -1)
    limit = 200

    return fetch_from_cache(query, context, sort, limit, 'latest_dns')


def fetch_latest_cidr():
    query = {'whois.asn_cidr': {'$exists': True}}
    context = {'_id': 0, 'whois.asn_country_code': 1, 'whois.asn_cidr': 1}
    sort = ('updated', -1)
    limit = 200

    return fetch_from_cache(query, context, sort, limit, 'latest_cidr')


def fetch_latest_ipv4():
    query = {'a_record': {'$exists': True}}
    context = {'_id': 0, 'a_record': 1, 'country_code': 1}
    sort = ('updated', -1)
    limit = 200

    return fetch_from_cache(query, context, sort, limit, 'ilatest_pv4')


def fetch_latest_asn():
    query = {'whois.asn': {'$exists': True}}
    context = {'_id': 0, 'whois.asn': 1, 'whois.asn_country_code': 1}
    sort = ('updated', -1)
    limit = 200

    return fetch_from_cache(query, context, sort, limit, 'latest_asn')


def asn_lookup(ipv4):
    asndb = pyasn.pyasn('rib.20191127.2000.dat', as_names_file='asn_names.json')
    asn, prefix = asndb.lookup(ipv4)
    name = asndb.get_as_name(asn)

    return {'prefix': prefix, 'name': name, 'asn': asn}


@app.route('/dns/<string:domain>', methods=['GET'])
def fetch_data_dns(domain):
    data = list(fetch_query_dns(domain))

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


@app.route('/dns/', methods=['GET'])
@app.route('/dns', methods=['GET'])
def fetch_latest_dns_data():
    data = fetch_latest_dns()

    if data:
        return jsonify(data)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


@app.route('/asn', methods=['GET'])
def fetch_latest_asn_data():
    data = fetch_latest_asn()

    if data:
        return jsonify(data)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


@app.route('/cidr', methods=['GET'])
def fetch_latest_cidr_data():
    data = fetch_latest_cidr()

    if data:
        return jsonify(data)
    else:
        return [{}], status.HTTP_404_NOT_FOUND


@app.route('/ipv4', methods=['GET'])
def fetch_latest_ipv4_data():
    data = fetch_latest_ipv4()
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
    parser.add_argument('--debug', help='debug flag', type=bool, default=False)
    parser.add_argument('--port', help='port number', type=int, required=True)
    args = parser.parse_args()

    return args


cache = connect_cache()

# create index for all match methods
create_index('ports.port', 'updated')
create_index('ssl_cert.subject.common_name', 'updated')
create_index('header.x-powered-by', 'updated')
create_index('banner', 'updated')
create_index('whois.asn', 'updated')
create_index('whois.asn_description', 'updated')
create_index('whois.asn_country_code', 'updated')
create_index('whois.asn_registry', 'updated')
create_index('whois.asn_cidr', 'updated')
create_index('cname_record.target', 'updated')
create_index('mx_record.exchange', 'updated')
create_index('header.server', 'updated')
create_index('header.status', 'updated')
create_index('a_record', 'updated')
create_index('domain', 'updated')


if __name__ == '__main__':
    args = argparser()
    app.run(port=args.port, debug=args.debug)
