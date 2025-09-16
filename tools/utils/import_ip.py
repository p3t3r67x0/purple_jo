#!/usr/bin/env python3

import argparse
import ipaddress

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


def load(i):
    with open(i) as f:
        return f.readlines()


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help='set input file name',
                        type=str, required=True)
    parser.add_argument('--host', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


def get_ips_from_cidr(cidr):
    """Return a list of IP addresses from a CIDR block."""
    return [str(ip) for ip in ipaddress.ip_network(cidr, strict=False).hosts()]


def main():
    args = argparser()
    client = connect(args.host)
    db = client.ip_data
    db.ipv4.create_index('ip', unique=True)

    for line in load(args.input):
        try:
            if '/' in line:
                # handle CIDR notation
                cidr = line.strip()
                # resolve CIDR and insert all IPs in the range
                for ip in get_ips_from_cidr(cidr):
                    db.ipv4.insert_one({'ip': ip})
                    print(ip)
            else:
                db.ipv4.insert_one({'ip': line.strip()})
                print(line.strip())
        except DuplicateKeyError as e:
            print(e)


if __name__ == '__main__':
    main()
