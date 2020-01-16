#!/usr/bin/env python3

from tools.extract_whois import handle_whois
from tools.extract_header import extract_header
from tools.extract_records import handle_records
from tools.extract_geodata import extract_geodata
from tools.extract_certificate import handle_certificate
from tools.generate_qrcode import generate_qrcode

from datetime import datetime
from pymongo import MongoClient


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_records(db, domain):
    return list(db.dns.find({'domain': domain}, {'a_record': 1}))


def update_header(db, domain):
    extract_header(db, domain, datetime.utcnow())


def update_records(db, domain):
    handle_records(db, domain, datetime.utcnow())


def update_qrcode(db, domain):
    generate_qrcode(db, domain, datetime.utcnow())


def update_certificate(db, domain):
    handle_certificate(db, domain, datetime.utcnow())


def update_geodata(db, ip, df):
    extract_geodata(db, ip, df)


def update_whois(db, ip):
    handle_whois(db, ip, datetime.utcnow())


def handle_query(domain, df):
    client = connect('localhost')
    db = client.ip_data

    update_records(db, domain)

    records = retrieve_records(db, domain)

    if len(records) > 0 and 'a_record' in records[0]:
        #update_whois(db, records[0]['a_record'][0])
        update_geodata(db, records[0]['a_record'][0], df)
        update_certificate(db, domain)
        update_header(db, domain)
        update_qrcode(db, domain)
