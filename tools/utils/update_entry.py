#!/usr/bin/env python3

from extract_whois import get_whois
from extract_header import fetch_header
from extract_records import handle_domain
from extract_geodata import extract_geodata
from ssl_cert_scanner import extract_certificate
from generate_qrcode import generate_qrcode

from datetime import datetime
from pymongo import MongoClient


def connect(host):
    return MongoClient(f"mongodb://{host}:27017")


def retrieve_records(db, domain):
    return list(db.dns.find({'domain': domain}, {'a_record': 1}))


def update_header(db, domain):
    fetch_header(db, domain, datetime.now())


def update_records(db, domain, type, record):
    handle_domain(db, domain, datetime.now(), type, record)


def update_qrcode(db, domain):
    generate_qrcode(db, domain, datetime.now())


def update_certificate(db, domain):
    extract_certificate(db, domain, datetime.now())


def update_geodata(db, ip, df):
    extract_geodata(db, ip, df)


def update_whois(db, ip):
    get_whois(db, ip, datetime.now())


def handle_query(domain, df, type=None, record=None):
    client = connect('localhost')
    db = client.ip_data

    update_records(db, domain, type, record)

    records = retrieve_records(db, domain)

    if len(records) > 0 and 'a_record' in records[0]:
        # update_whois(db, records[0]['a_record'][0])
        update_geodata(db, records[0]['a_record'][0], df)
        update_certificate(db, domain)
        update_header(db, domain)
        update_qrcode(db, domain)
