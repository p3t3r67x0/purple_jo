import logging
import re

from datetime import datetime, timedelta

from app.services import (
    perform_live_scan
)

from app.api.utils import (
    cache_key,
    fetch_from_cache
)

from asyncio.log import logger

logger.setLevel(logging.INFO)


async def fetch_query_domain(mongo, domain: str):
    sub_query = domain.lower()
    query = {"$text": {"$search": domain}}
    filter = {"_id": 0}
    sort = {"score": {"$meta": "textScore"}}
    limit = 30
    context = "text"
    return await fetch_from_cache(
        mongo,
        query=query,
        filter=filter,
        unwind=False,
        sort=sort,
        limit=limit,
        context=context,
        reset=False,
        cache_key_str=f"all-{cache_key(sub_query)}",
    )


async def fetch_all_prefix(mongo, prefix: str):
    cursor = mongo.lookup.find({'cidr': {'$in': [prefix]}}, {'_id': 0})
    return await cursor.to_list(length=100)


async def fetch_latest_dns(mongo, page: int = 1, page_size: int = 10):
    date = datetime.now() - timedelta(days=1)
    query = {"updated": {"$gte": date}}
    filter = {"_id": 0}
    sort = {"updated": -1}
    context = "normal"
    skip = (page - 1) * page_size

    results = await fetch_from_cache(
        mongo,
        query=query,
        filter=filter,
        unwind=False,
        sort=sort,
        limit=page_size,
        skip=skip,
        context=context,
        reset=False,
        cache_key_str=f"latest_dns_{page}_{page_size}",
    )

    return {"page": page, "page_size": page_size, "results": results}


async def fetch_latest_cidr(mongo):
    query = {'whois.asn_cidr': {'$exists': True}}
    filter = {'_id': 0, 'whois.asn_country_code': 1, 'whois.asn_cidr': 1}
    sort = {'updated': -1}
    limit = 200
    context = "normal"
    return await fetch_from_cache(
        mongo,
        query,
        filter,
        unwind=False,
        sort=sort,
        limit=limit,
        context=context,
        reset=False,
        cache_key_str="latest_cidr",
    )


async def fetch_latest_ipv4(mongo, page: int = 1, page_size: int = 60):
    skip = (page - 1) * page_size
    pipeline = [
        {"$match": {"a_record": {"$exists": True, "$ne": []}}},
        {"$sort": {"updated": -1}},
        {"$skip": skip},
        {"$limit": page_size},
        {"$unwind": "$a_record"},
        {"$project": {"_id": 0, "a_record": 1, "country_code": 1}}
    ]
    cursor = mongo.dns.aggregate(pipeline)
    results = [doc async for doc in cursor]
    return {"page": page, "page_size": page_size, "results": results}


async def fetch_latest_asn(mongo):
    query = {'whois.asn': {'$exists': True}}
    filter = {'_id': 0, 'whois.asn': 1, 'whois.asn_country_code': 1}
    sort = {'updated': -1}
    limit = 200
    context = "normal"
    return await fetch_from_cache(
        mongo,
        query,
        filter,
        unwind=False,
        sort=sort,
        limit=limit,
        context=context,
        reset=False,
        cache_key_str="latest_asn",
    )


async def fetch_one_ip(mongo, ip: str):
    cursor = mongo.dns.find({'a_record': {'$in': [ip]}}, {'_id': 0})
    return await cursor.to_list(length=1)


async def extract_graph(db, domain):
    """
    Same logic as before, just moved here for graph queries.
    """
    cursor = db.dns.aggregate([
        {'$match': {'domain': domain}},
        {'$graphLookup': {
            'from': 'dns',
            'startWith': '$ssl.subject_alt_names',
            'connectFromField': 'domain',
            'connectToField': 'ssl.subject_alt_names',
            'as': 'certificates'
        }},
        {'$graphLookup': {
            'from': 'dns',
            'startWith': '$cname_record.target',
            'connectFromField': 'domain',
            'connectToField': 'cname_record.target',
            'as': 'cname_records'
        }},
        {'$graphLookup': {
            'from': 'dns',
            'startWith': '$mx_record.exchange',
            'connectFromField': 'mx_record.exchange',
            'connectToField': 'domain',
            'as': 'mx_records'
        }},
        {'$graphLookup': {
            'from': 'dns',
            'startWith': '$ns_record',
            'connectFromField': 'ns_record',
            'connectToField': 'domain',
            'as': 'ns_records'
        }},
        {'$project': {
            'main.domain': '$domain',
            'main.a_record': '$a_record',
            'zzz': {'$setUnion': [
                '$certificates', '$cname_records', '$mx_records', '$ns_records'
            ]}
        }},
        {'$unwind': '$zzz'},
        {'$group': {
            '_id': '$_id',
            'main': {'$addToSet': '$main'},
            'all': {'$addToSet': '$zzz'}
        }},
        {'$project': {
            'all.domain': 1,
            'all.a_record': 1,
            'main': 1,
            '_id': 0
        }}
    ])
    return await cursor.to_list(length=None)


async def fetch_match_condition(mongo, condition: str, query: str):
    if not query:
        return []

    print(f"Fetching condition: {condition} with query: {query}")

    condition = condition.lower()

    # Normalizers
    sub_query = query.lower()
    if condition == "country":
        sub_query = query.upper()

    def _build_geo_query(q: str):
        try:
            lat, lon = map(float, q.split(","))
            return {
                "geo.loc": {
                    "$nearSphere": {
                        "$geometry": {
                            "type": "Point",
                            "coordinates": [lat, lon]
                        },
                        "$maxDistance": 50000,
                    }
                }
            }
        except ValueError:
            return {}

    # Dispatcher map
    condition_map = {
        "registry": lambda q: {"whois.asn_registry": q},
        "port": lambda q: {"ports.port": int(q)},
        "status": lambda q: {"header.status": q},
        "ssl": lambda q: {
            "$or": [
                {"ssl.subject.common_name": q},
                {"ssl.subject_alt_names": {"$in": [q]}},
            ]
        },
        "before": lambda q: {
            "ssl.not_before": {
                "$gte": datetime.strptime(q, "%Y-%m-%d %H:%M:%S")
            }
        },
        "after": lambda q: {
            "ssl.not_after": {
                "$lte": datetime.strptime(q, "%Y-%m-%d %H:%M:%S")
            }
        },
        "ca": lambda q: {"ssl.ca_issuers": q},
        "issuer": lambda q: {"$or": [
            {"ssl.issuer.organization_name": q},
            {"ssl.issuer.common_name": q}
        ]},
        "unit": lambda q: {"$or": [
            {"ssl.issuer.organizational_unit_name": q},
            {"ssl.subject.organizational_unit_name": q}
        ]},
        "ocsp": lambda q: {"ssl.ocsp": q},
        "crl": lambda q: {"ssl.crl_distribution_points": q},
        "service": lambda q: {"header.x-powered-by": q},
        "country": lambda q: {"$or": [
            {"geo.country_code": q},
            {"whois.asn_country_code": q}
        ]},
        "state": lambda q: {
            "$expr": {
                "$regexMatch": {
                    "input": {"$toLower": "$state"},
                    "regex": q.lower()
                }
            }
        },
        "city": lambda q: {
            "$expr": {
                "$regexMatch": {
                    "input": {"$toLower": "$city"},
                    "regex": q.lower()
                }
            }
        },
        "loc": _build_geo_query,
        "banner": lambda q: {
            "$expr": {
                "$regexMatch": {
                    "input": {"$toLower": "$banner"},
                    "regex": q.lower()
                }
            }
        },
        "asn": lambda q: {"whois.asn": re.sub(r"[a-zA-Z:]", "", q.lower())},
        "org": lambda q: {
            "$or": [
                {
                    "$expr": {
                        "$regexMatch": {
                            "input": {"$toLower": "$whois.asn_description"},
                            "regex": q.lower()
                        }
                    }
                },
                {
                    "$expr": {
                        "$regexMatch": {
                            "input": {
                                "$toLower": "$ssl.subject.organization_name"
                            },
                            "regex": q.lower()
                        }
                    }
                }
            ]
        },
        "cidr": lambda q: {"whois.asn_cidr": q},
        "cname": lambda q: {"cname_record.target": {"$in": [q]}},
        "mx": lambda q: {"mx_record.exchange": {"$in": [q]}},
        "ns": lambda q: {"ns_record": {"$in": [q]}},
        "server": lambda q: {
            "$expr": {
                "$regexMatch": {
                    "input": {"$toLower": "$header.server"},
                    "regex": q.lower()
                }
            }
        },
        "site": lambda q: {"domain": q},
        "ipv4": lambda q: {"a_record": {"$in": [q]}},
        "ipv6": lambda q: {"aaaa_record": {"$in": [q]}},
    }

    # Build query
    builder = condition_map.get(condition)
    print(f"Using builder for condition: {condition}")
    if not builder:
        return []

    mongo_query = builder(sub_query)
    print(f"MongoDB Query: {mongo_query}")

    cursor = mongo.dns.find(mongo_query, {"_id": 0}).sort(
        "updated", -1).limit(30)
    results = await cursor.to_list(length=30)
    print(f"Found {len(results)} results in DB for condition: {condition}")
    
    only_domains = bool(results) and all(
        isinstance(doc.get("a_record"), list) and len(doc["a_record"]) >= 1
        for doc in results
    )
    
    print(only_domains)

    # Fallback live-scan if nothing OR only bare domains
    # (keep your condition guard)
    if (not results or not only_domains) and condition in ["site", "domain"]:
        return [await perform_live_scan(mongo, query)]

    return results
