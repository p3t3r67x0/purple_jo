#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ssl
import socket
import OpenSSL

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from OpenSSL.crypto import Error as CryptoError
from datetime import datetime

socket.setdefaulttimeout(1)


def connect():
    return MongoClient('mongodb://127.0.0.1:27017')


def retrieve_domains(db):
    return db.dns.find({'domain': {'$exists': True},
                        'ssl_cert': {'$exists': False},
                        'cert_scan_failed': {'$exists': False}}).sort([('$natural', -1)])


def update_data(db, doc_id, domain, post):
    try:
        db.dns.update_one({'_id': doc_id}, {'$set': post}, upsert=False)
        print(u'INFO: updated domain {} ssl cert'.format(domain))
    except DuplicateKeyError:
        pass


def extract_certificate(domain):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        cert = ssl.get_server_certificate((domain, 443))
        x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, cert)
    except (CryptoError, socket.error):
        return

    extensions = {}
    subject = {}
    issuer = {}

    for item in x509.get_subject().get_components():
        subject[item[0].lower().decode('utf-8')] = item[1].decode('utf-8')

    for item in x509.get_issuer().get_components():
        issuer[item[0].lower().decode('utf-8')] = item[1].decode('utf-8')

    for i in range(x509.get_extension_count()):
        try:
            extensions[x509.get_extension(i).get_short_name().lower().decode('utf-8')] = x509.get_extension(i).__str__()
        except CryptoError:
            pass

    post = [{
        'cert': cert,
        'expired': x509.has_expired(),
        'algorithm': x509.get_signature_algorithm().decode('utf-8'),
        'hash': {
            'md5': x509.digest('md5').decode('utf-8'),
            'sha1': x509.digest('sha1').decode('utf-8'),
            'sha224': x509.digest('sha224').decode('utf-8'),
            'sha256': x509.digest('sha256').decode('utf-8'),
            'sha384': x509.digest('sha384').decode('utf-8'),
            'sha512': x509.digest('sha512').decode('utf-8')
        },
        'not_before': x509.get_notBefore().decode('utf-8'),
        'not_after': x509.get_notAfter().decode('utf-8'),
        'subject': subject,
        'issuer': issuer,
        'extensions': extensions,
        'serial': str(x509.get_serial_number()),
        'bits': x509.get_pubkey().bits(),
        'type': x509.get_pubkey().type()
    }]

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
