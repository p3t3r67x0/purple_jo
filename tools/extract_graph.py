#!/usr/bin/env python3

import pyasn
import multiprocessing
import argparse

from datetime import datetime
from pymongo import MongoClient


def connect(host):
    return MongoClient('mongodb://{}:27017'.format(host))


def retrieve_entries(db, domain):
    return db.dns.aggregate([{'$match': {'domain': domain}},
                             {'$graphLookup': {'from': 'dns', 'startWith': '$mx_record.exchange',
                                               'connectFromField': 'mx_record.exchange', 'connectToField': 'domain', 'as': 'mx_records'}},
                             {'$graphLookup': {'from': 'dns', 'startWith': '$ns_record', 'connectFromField': 'ns_record',
                                               'connectToField': 'domain', 'maxDepth': 0, 'as': 'ns_records'}},
                             {'$graphLookup': {'from': 'dns', 'startWith': '$a_record',
                                               'connectFromField': 'unknown', 'connectToField': 'a_record', 'as': 'a_records'}},
                             {'$graphLookup': {'from': 'dns', 'startWith': '$ssl.subject_alt_names',
                                               'connectFromField': 'domain', 'connectToField': 'ssl.subject_alt_names', 'as': 'certificates'}},
                             {'$graphLookup': {'from': 'dns', 'startWith': '$cname_record.target',
                                               'connectFromField': 'unknown', 'connectToField': 'cname_record.target', 'as': 'cname_records'}},
                             {'$project': {
                                 'a_records.a_record': 1,
                                 'a_records.domain': 1,
                                 'ns_records.a_record': 1,
                                 'ns_records.domain': 1,
                                 'mx_records.a_record': 1,
                                 'mx_records.domain': 1,
                                 'cname_records.a_record': 1,
                                 'cname_records.domain': 1,
                                 'certificates.a_record': 1,
                                 'certificates.domain': 1,
                                 '_id': 0}}
                             ])


def update_summary(summary, data):
    if 'a_record' in data:
        summary.add((data['domain'], ','.join(data['a_record'])))
    else:
        summary.add((data['domain'], ''))

    return summary


def extract_graph(db, domain):
    results = list(retrieve_entries(db, domain))
    summary_s = set()
    edges_s = set()
    groups = set()
    groups_d = {}
    nodes = []
    edges = []
    main = None

    if len(results) > 0:
        for i in results[0]['a_records']:
            other = update_summary(summary_s, i)
            summary_s.union(other)

            if 'a_record' in i:
                for j in i['a_record']:
                    groups.add(j)

        for i in results[0]['cname_records']:
            other = update_summary(summary_s, i)
            summary_s.union(other)

            if 'a_record' in i:
                for j in i['a_record']:
                    groups.add(j)

        for i in results[0]['mx_records']:
            other = update_summary(summary_s, i)
            summary_s.union(other)

            if 'a_record' in i:
                for j in i['a_record']:
                    groups.add(j)

        for i in results[0]['ns_records']:
            other = update_summary(summary_s, i)
            summary_s.union(other)

            if 'a_record' in i:
                for j in i['a_record']:
                    groups.add(j)

        for i in results[0]['certificates']:
            other = update_summary(summary_s, i)
            summary_s.union(other)

            if 'a_record' in i:
                for j in i['a_record']:
                    groups.add(j)


        for i, v in enumerate(groups):
            groups_d[v] = i


        for i, v in enumerate(summary_s):
            if v[0] == domain:
                main = i

            g = []
            e = {}
            o = {}
            o['id'] = i
            o['label'] = v[0]
            e['from'] = i

            for j in v[1].split(','):
                if j in groups:
                    g.append(str(groups_d[j]))
                    e['to'] = groups_d[j]

                    if e['from'] != e['to'] and (e['to'], e['from']) not in edges_s:
                        edges_s.add((e['from'], e['to']))
                        o['group'] = str(g[0])

            nodes.append(o)


    for i, v in enumerate(nodes):
        e = {}
        e['from'] = main
        e['to'] = v['id']

        if e['from'] != e['to'] and (e['to'], e['from']) not in edges_s:
            edges_s.add((e['from'], e['to']))


    for i in edges_s:
        e = {}
        e['from'] = i[0]
        e['to'] = i[1]

        edges.append(e)


    return {'nodes': nodes, 'edges': edges}


def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help='set the host', type=str, required=True)
    parser.add_argument('--domain', help='set the host', type=str, required=True)
    args = parser.parse_args()

    return args


def main():
    args = argparser()
    client = connect(args.host)
    db = client.ip_data

    print(extract_graph(db, args.domain))


if __name__ == '__main__':
    main()
