#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import ssl
import socket

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from datetime import datetime
from ssl import SSLError


socket.setdefaulttimeout(1)


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def retrieve_domains(db):
    return db.dns.find({'ssl_cert': {'$exists': False},
                        'cert_scan_failed': {'$exists': False}
                        }).sort([('$natural', -1)])


def update_data(db, doc_id, domain, post):
    try:
        db.dns.update_one({'_id': doc_id}, {'$set': post}, upsert=False)
        print(u'INFO: updated domain {} ssl cert'.format(domain))
    except DuplicateKeyError:
        pass


def extract_certificate(domain):
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = False
    context.load_default_certs()
    context.set_ciphers('ECDHE+AESGCM:!ECDSA')

    s = context.wrap_socket(socket.socket(), server_hostname=domain)

    try:
        s.connect((domain, 443))
        cert = s.getpeercert(binary_form=False)
        cipher = s.shared_ciphers()
        s.close()
    except (ConnectionRefusedError, OSError, SSLError, socket.gaierror, socket.timeout):
        return

    issuer = {}
    subject = {}
    alt_names = []
    ciphers = []

    for item in cert['subject']:
        subject['_'.join(re.findall('.[^A-Z]*', item[0][0])).lower()] = item[0][1]

    for item in cert['issuer']:
        issuer['_'.join(re.findall('.[^A-Z]*', item[0][0])).lower()] = item[0][1]

    for item in cert['subjectAltName']:
        alt_names.append(item[1])

    for item in cipher:
        ciphers.append({'tls_version': item[1], 'name': item[0], 'bits': item[2]})

    post = [{
        'issuer': issuer,
        'ciphers': ciphers,
        'subject': subject,
        'subject_alt_names': alt_names,
        'serial': cert['serialNumber'],
        'not_before': cert['notBefore'],
        'not_after': cert['notAfter'],
        'version': cert['version'],
    }]

    if 'OCSP' in cert and len(cert['OCSP']):
        post[0]['ocsp'] = cert['OCSP'][0]

    if 'caIssuers' in cert and len(cert['caIssuers']):
        post[0]['ca_issuers'] = cert['caIssuers'][0]

    if 'crlDistributionPoints' in cert and len(cert['crlDistributionPoints']):
        post[0]['crl_distribution_points'] = cert['crlDistributionPoints'][0]

    return post


def main():
    client = connect()
    db = client.ip_data

    for domain in retrieve_domains(db):
        print(u'INFO: scanning domain {} for ssl certificate'.format(domain['domain']))

        cert = extract_certificate(domain['domain'])

        if cert:
            print(cert)
            update_data(db, domain['_id'], domain['domain'], {'ssl_cert': cert, 'updated': datetime.utcnow()})
        else:
            update_data(db, domain['_id'], domain['domain'], {'cert_scan_failed': datetime.utcnow()})


if __name__ == '__main__':
    main()
