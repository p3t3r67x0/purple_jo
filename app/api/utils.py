# app/api/utils.py
from bson import ObjectId
from app.cache import cache


async def fetch_from_cache(mongo, query, filter=None, unwind=False, sort=None,
                           limit=None, skip=0, context="normal", reset=False,
                           cache_key_str=None):
    """
    Generic helper to fetch from cache or query Mongo as fallback.
    """
    key = cache_key_str or str(query)
    data = await cache.get(key)

    if data and not reset:
        return data

    # Build Mongo query
    cursor = mongo.dns.find(query, filter or {})
    if sort:
        cursor = cursor.sort(list(sort.items()))
    if skip:
        cursor = cursor.skip(skip)
    if limit:
        cursor = cursor.limit(limit)

    results = await cursor.to_list(length=limit or 100)

    # Cache results
    await cache.set(key, results, ttl=300)
    return results


def fix_mongo_ids(doc):
    """
    Recursively convert ObjectId fields in MongoDB documents to strings
    so they can be serialized as JSON.
    """
    if isinstance(doc, list):
        return [fix_mongo_ids(d) for d in doc]
    if isinstance(doc, dict):
        return {k: fix_mongo_ids(v) for k, v in doc.items()}
    if isinstance(doc, ObjectId):
        return str(doc)
    return doc


def cache_key(key: str) -> str:
    import re
    return re.sub(r'[\\\/\(\)\'\"\[\],;:#+~\. ]', '-', key)


def extra_fields(context: str) -> dict:
    data = {
        'created_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$created'}},
        'updated_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$updated'}},
        'domain_crawled_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$domain_crawled'}},
        'header_scan_failed_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$header_scan_failed'}},
        'ssl.not_after_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$ssl.not_after'}},
        'ssl.not_before_formatted': {'$dateToString': {'format': '%Y-%m-%d %H:%M:%S', 'date': '$ssl.not_before'}}
    }
    if context == 'text':
        data['score'] = {'$meta': "textScore"}
    return data
