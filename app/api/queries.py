import logging
import re
import socket
from datetime import datetime, timedelta
from typing import Optional, Tuple

from asyncio.log import logger

from app.api.utils import cache_key, cached_paginated_fetch, paginate
from app.services import asn_lookup, perform_live_scan

logger.setLevel(logging.INFO)

DEFAULT_CACHE_TTL = 300


def _pagination_bounds(page: int, page_size: int) -> Tuple[int, int]:
    safe_page = max(page, 1)
    safe_page_size = max(page_size, 1)
    skip = (safe_page - 1) * safe_page_size
    return skip, safe_page_size


async def fetch_query_domain(mongo, domain: str, page: int = 1, page_size: int = 25):
    """Full text search across the DNS collection with caching and pagination."""

    sub_query = domain.lower()
    query = {"$text": {"$search": domain}}
    projection = {"_id": 0, "score": {"$meta": "textScore"}}
    skip, limit = _pagination_bounds(page, page_size)

    async def loader():
        cursor = mongo.dns.find(query, projection).sort(
            [("score", {"$meta": "textScore"}), ("updated", -1)]
        )
        if skip:
            cursor = cursor.skip(skip)
        cursor = cursor.limit(limit)

        results = await cursor.to_list(length=limit)
        total = await mongo.dns.count_documents(query)
        return results, total

    key = f"query:{cache_key(sub_query)}:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def fetch_all_prefix(mongo, prefix: str, page: int = 1, page_size: int = 25):
    """Lookup CIDR prefixes from the cached subnet collection."""

    query = {"cidr": {"$in": [prefix]}}
    skip, limit = _pagination_bounds(page, page_size)

    async def loader():
        cursor = (
            mongo.lookup.find(query, {"_id": 0})
            .sort("_id", -1)
            .skip(skip)
            .limit(limit)
        )
        results = await cursor.to_list(length=limit)
        total = await mongo.lookup.count_documents(query)
        return results, total

    key = f"subnet:{cache_key(prefix)}:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def fetch_latest_dns(mongo, page: int = 1, page_size: int = 10):
    """Return recently updated DNS documents with pagination."""

    query = {"a_record": {"$exists": True, "$ne": []}}
    skip, limit = _pagination_bounds(page, page_size)

    async def loader():
        cursor = (
            mongo.dns.find(query, {"_id": 0})
            .sort("updated", -1)
            .skip(skip)
            .limit(limit)
        )
        results = await cursor.to_list(length=limit)
        total = await mongo.dns.count_documents(query)
        return results, total

    key = f"dns:latest:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def fetch_latest_cidr(mongo, page: int = 1, page_size: int = 50):
    """Return the latest ASN CIDR ranges."""

    query = {"whois.asn_cidr": {"$exists": True}}
    projection = {"_id": 0, "whois.asn_country_code": 1, "whois.asn_cidr": 1}
    skip, limit = _pagination_bounds(page, page_size)

    async def loader():
        cursor = (
            mongo.dns.find(query, projection)
            .sort("updated", -1)
            .skip(skip)
            .limit(limit)
        )
        results = await cursor.to_list(length=limit)
        total = await mongo.dns.count_documents(query)
        return results, total

    key = f"cidr:latest:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def fetch_latest_ipv4(mongo, page: int = 1, page_size: int = 60):
    """Return recent IPv4 records with pagination."""

    match_stage = {"a_record": {"$exists": True, "$ne": []}}
    skip, limit = _pagination_bounds(page, page_size)

    async def loader():
        pipeline = [
            {"$match": match_stage},
            {"$unwind": "$a_record"},
            {"$sort": {"updated": -1}},
            {"$skip": skip},
            {"$limit": limit},
            {"$project": {"_id": 0, "a_record": 1, "country_code": 1}},
        ]
        cursor = mongo.dns.aggregate(pipeline)
        results = [doc async for doc in cursor]

        count_cursor = mongo.dns.aggregate(
            [
                {"$match": match_stage},
                {"$unwind": "$a_record"},
                {"$count": "count"},
            ]
        )
        count_docs = await count_cursor.to_list(length=1)
        total = count_docs[0]["count"] if count_docs else 0
        return results, total

    key = f"ipv4:latest:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def fetch_latest_asn(
    mongo,
    page: int = 1,
    page_size: int = 50,
    country_code: Optional[str] = None,
):
    """Return the most recently seen ASNs, optionally filtered by country code."""

    query = {"whois.asn": {"$exists": True}}
    if country_code:
        query["whois.asn_country_code"] = country_code.upper()
    projection = {
        "_id": 0,
        "whois.asn": 1,
        "whois.asn_country_code": 1,
        "whois.asn_description": 1,
    }
    skip, limit = _pagination_bounds(page, page_size)

    async def loader():
        cursor = (
            mongo.dns.find(query, projection)
            .sort("updated", -1)
            .skip(skip)
            .limit(limit)
        )
        results = await cursor.to_list(length=limit)
        total = await mongo.dns.count_documents(query)
        return results, total

    country_key = country_code.upper() if country_code else "all"
    key = f"asn:latest:{country_key}:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def _build_ip_fallback(mongo, ip: str) -> dict:
    """Construct a fallback record for IP lookups when Mongo has no entry."""

    lookup = asn_lookup(ip)
    try:
        host = socket.gethostbyaddr(ip)[0]
    except Exception:  # noqa: BLE001 - best effort reverse lookup
        host = None

    now = datetime.now()
    record = {
        "ip": ip,
        "host": host,
        "updated": now,
        "asn": lookup.get("asn"),
        "name": lookup.get("name"),
        "cidr": [lookup.get("prefix")],
    }

    try:
        await mongo.lookup.update_one({"ip": ip}, {"$set": record}, upsert=True)
    except Exception:  # noqa: BLE001 - cache insert best effort
        pass

    return record


async def fetch_one_ip(mongo, ip: str, page: int = 1, page_size: int = 1):
    """Return paginated information for a specific IPv4 address."""

    query = {"a_record": {"$in": [ip]}}
    projection = {"_id": 0}
    skip, limit = _pagination_bounds(page, page_size)

    async def loader():
        cursor = mongo.dns.find(query, projection).sort("updated", -1)
        if skip:
            cursor = cursor.skip(skip)
        cursor = cursor.limit(limit)

        results = await cursor.to_list(length=limit)
        total = await mongo.dns.count_documents(query)

        if not results and page == 1:
            logger.info("Falling back to live ASN lookup for %s", ip)
            results = [await _build_ip_fallback(mongo, ip)]
            total = 1

        return results, total

    key = f"ip:{ip}:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def extract_graph(db, domain: str, page: int = 1, page_size: int = 50):
    """Return a graph of related DNS entities for a given domain."""

    pipeline = [
        {"$match": {"domain": domain}},
        {
            "$graphLookup": {
                "from": "dns",
                "startWith": "$ssl.subject_alt_names",
                "connectFromField": "domain",
                "connectToField": "ssl.subject_alt_names",
                "as": "certificates",
            }
        },
        {
            "$graphLookup": {
                "from": "dns",
                "startWith": "$cname_record.target",
                "connectFromField": "domain",
                "connectToField": "cname_record.target",
                "as": "cname_records",
            }
        },
        {
            "$graphLookup": {
                "from": "dns",
                "startWith": "$mx_record.exchange",
                "connectFromField": "mx_record.exchange",
                "connectToField": "domain",
                "as": "mx_records",
            }
        },
        {
            "$graphLookup": {
                "from": "dns",
                "startWith": "$ns_record",
                "connectFromField": "ns_record",
                "connectToField": "domain",
                "as": "ns_records",
            }
        },
        {
            "$project": {
                "main.domain": "$domain",
                "main.a_record": "$a_record",
                "zzz": {
                    "$setUnion": ["$certificates", "$cname_records", "$mx_records", "$ns_records"]
                },
            }
        },
        {"$unwind": "$zzz"},
        {"$group": {"_id": "$_id", "main": {"$addToSet": "$main"}, "all": {"$addToSet": "$zzz"}}},
        {"$project": {"all.domain": 1, "all.a_record": 1, "main": 1, "_id": 0}},
    ]

    skip, limit = _pagination_bounds(page, page_size)

    async def loader():
        cursor = db.dns.aggregate(pipeline)
        docs = [doc async for doc in cursor]
        total = len(docs)
        start = min(skip, total)
        end = min(start + limit, total)
        return docs[start:end], total

    key = f"graph:{cache_key(domain.lower())}:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )


async def fetch_match_condition(
    mongo,
    condition: str,
    query: str,
    page: int = 1,
    page_size: int = 30,
):
    """Dispatch match queries with pagination and Redis caching."""

    if not query:
        return paginate(page=page, page_size=page_size, total=0, results=[])

    condition = condition.lower()
    logger.info("Fetching condition: %s with query: %s", condition, query)

    sub_query = query.lower()
    if condition == "country":
        sub_query = query.upper()

    def _build_geo_query(q: str):
        try:
            lat, lon = map(float, q.split(","))
            return {
                "geo.loc": {
                    "$nearSphere": {
                        "$geometry": {"type": "Point", "coordinates": [lat, lon]},
                        "$maxDistance": 50000,
                    }
                }
            }
        except ValueError:
            return {}

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
        "before": lambda q: {"ssl.not_before": {"$gte": datetime.strptime(q, "%Y-%m-%d %H:%M:%S")}},
        "after": lambda q: {"ssl.not_after": {"$lte": datetime.strptime(q, "%Y-%m-%d %H:%M:%S")}},
        "ca": lambda q: {"ssl.ca_issuers": q},
        "issuer": lambda q: {
            "$or": [
                {
                    "$expr": {
                        "$eq": [
                            {"$toLower": "$ssl.issuer.organization_name"},
                            q.lower(),
                        ]
                    }
                },
                {
                    "$expr": {
                        "$eq": [
                            {"$toLower": "$ssl.issuer.common_name"},
                            q.lower(),
                        ]
                    }
                },
            ]
        },
        "unit": lambda q: {
            "$or": [
                {"ssl.issuer.organizational_unit_name": q},
                {"ssl.subject.organizational_unit_name": q},
            ]
        },
        "ocsp": lambda q: {"ssl.ocsp": q},
        "crl": lambda q: {"ssl.crl_distribution_points": q},
        "service": lambda q: {"header.x-powered-by": q},
        "country": lambda q: {"$or": [{"geo.country_code": q}, {"whois.asn_country_code": q}]},
        "state": lambda q: {
            "$expr": {
                "$regexMatch": {
                    "input": {"$toLower": "$geo.state"},
                    "regex": q.lower(),
                }
            }
        },
        "city": lambda q: {
            "$expr": {
                "$regexMatch": {
                    "input": {"$toLower": "$geo.city"},
                    "regex": q.lower(),
                }
            }
        },
        "loc": _build_geo_query,
        "banner": lambda q: {
            "$expr": {
                "$regexMatch": {
                    "input": {"$toLower": "$banner"},
                    "regex": q.lower(),
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
                            "regex": q.lower(),
                        }
                    }
                },
                {
                    "$expr": {
                        "$regexMatch": {
                            "input": {"$toLower": "$ssl.subject.organization_name"},
                            "regex": q.lower(),
                        }
                    }
                },
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
                    "regex": q.lower(),
                }
            }
        },
        "site": lambda q: {"domain": q},
        "ipv4": lambda q: {"a_record": {"$in": [q]}},
        "ipv6": lambda q: {"aaaa_record": {"$in": [q]}},
    }

    builder = condition_map.get(condition)
    logger.info("Using builder for condition: %s", condition)
    if not builder:
        return paginate(page=page, page_size=page_size, total=0, results=[])

    mongo_query = builder(sub_query)
    if not mongo_query:
        return paginate(page=page, page_size=page_size, total=0, results=[])
    logger.info("MongoDB Query: %s", mongo_query)
    skip, limit = _pagination_bounds(page, page_size)

    async def loader():
        cursor = mongo.dns.find(mongo_query, {"_id": 0}).sort("updated", -1)
        if skip:
            cursor = cursor.skip(skip)
        cursor = cursor.limit(limit)
        results = await cursor.to_list(length=limit)
        total = await mongo.dns.count_documents(mongo_query)

        only_domains = bool(results) and all(
            isinstance(doc.get("a_record"), list) and len(doc["a_record"]) >= 1
            for doc in results
        )

        if (not results or not only_domains) and condition in {"site", "domain"} and page == 1:
            logger.info("Running live scan fallback for %s", query)
            results = [await perform_live_scan(mongo, query)]
            total = 1

        logger.info("Returning %s results for condition %s", len(results), condition)
        return results, total

    key = f"match:{condition}:{cache_key(sub_query)}:{page}:{page_size}"
    return await cached_paginated_fetch(
        key,
        loader,
        page=page,
        page_size=page_size,
        ttl=DEFAULT_CACHE_TTL,
    )
