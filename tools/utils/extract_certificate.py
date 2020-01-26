#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import ssl
import socket
import argparse

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.errors import NotMasterError

from datetime import datetime
from ssl import SSLError


socket.setdefaulttimeout(1)


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_domains(db):
    return db.dns.find({'ssl': {'$exists': False}, 'domain': {
                        '$regex': '^(([\w]*\.)?(?!(xn--)+)[\w]*\.[\w]+)$'},
                        'cert_scan_failed': {'$exists': False},
                        'ports.port': {'$in': [443]}
                        }).sort([('updated', -1)])


def update_data(db, domain, post):
    try:
        db.dns.update_one({'domain': domain}, {'$set': post}, upsert=False)
        print(u'INFO: updated domain {} ssl cert'.format(domain))
    except (ServerSelectionTimeoutError, NotMasterError, DuplicateKeyError):
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

    for item in cert['subject']:
        subject['_'.join(re.findall('.[^A-Z]*', item[0][0])
                         ).lower()] = item[0][1]

    for item in cert['issuer']:
        issuer['_'.join(re.findall('.[^A-Z]*', item[0][0])
                        ).lower()] = item[0][1]

    for item in cert['subjectAltName']:
        alt_names.append(item[1])

    post = {
        'issuer': issuer,
        'subject': subject,
        'subject_alt_names': alt_names,
        'serial': cert['serialNumber'],
        'not_before': datetime.strptime(cert['notBefore'], '%b %d %H:%M:%S %Y %Z'),
        'not_after': datetime.strptime(cert['notAfter'], '%b %d %H:%M:%S %Y %Z'),
        'version': cert['version'],
    }

    if 'OCSP' in cert and len(cert['OCSP']):
        post['ocsp'] = cert['OCSP'][0].strip('/')

    if 'caIssuers' in cert and len(cert['caIssuers']):
        post['ca_issuers'] = cert['caIssuers'][0].strip('/')

    if 'crlDistributionPoints' in cert and len(cert['crlDistributionPoints']):
        post['crl_distribution_points'] = cert['crlDistributionPoints'][0].strip('/')

    return post


def handle_certificate(db, domain, date):
    cert = extract_certificate(domain)

    if cert:
        update_data(db, domain, {'ssl': cert, 'updated': date})
    else:
        update_data(db, domain, {'ssl_scan_failed': date})


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


def main():
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    for domain in retrieve_domains(db):
        handle_certificate(db, domain['domain'], datetime.utcnow())

    client.close()


if __name__ == '__main__':
    main()
