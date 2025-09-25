from typing import Any, Mapping

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import GEOSPHERE, IndexModel, TEXT

from app.config import DB_NAME, MONGO_URI


mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]
TEXT_INDEX_FIELDS = (
    ("domain", TEXT),
    ("header.x-powered-by", TEXT),
    ("banner", TEXT),
    ("ports.port", TEXT),
    ("whois.asn", TEXT),
    ("whois.asn_description", TEXT),
    ("whois.asn_country_code", TEXT),
    ("whois.asn_registry", TEXT),
    ("whois.asn_cidr", TEXT),
    ("cname_record.target", TEXT),
    ("mx_record.exchange", TEXT),
    ("header.server", TEXT),
    ("header.status", TEXT),
    ("ns_record", TEXT),
    ("aaaa_record", TEXT),
    ("a_record", TEXT),
    ("geo.country_code", TEXT),
    ("geo.country", TEXT),
    ("geo.state", TEXT),
    ("geo.city", TEXT),
    ("ssl.ocsp", TEXT),
    ("ssl.not_after", TEXT),
    ("ssl.not_before", TEXT),
    ("ssl.ca_issuers", TEXT),
    ("ssl.issuer.common_name", TEXT),
    ("ssl.issuer.organization_name", TEXT),
    ("ssl.issuer.organizational_unit_name", TEXT),
    ("ssl.subject_alt_names", TEXT),
    ("ssl.subject.common_name", TEXT),
    ("ssl.subject.organizational_unit_name", TEXT),
    ("ssl.subject.organization_name", TEXT),
    ("ssl.crl_distribution_points", TEXT),
)

TEXT_INDEX_NAME = "all_fields_text_index"

DOMAIN_UNIQUE_INDEX = IndexModel(
    [("domain", 1)],
    name="domain_unique_index",
    unique=True,
)

A_RECORD_SPARSE_INDEX = IndexModel(
    [("a_record", 1)],
    name="a_record_sparse_index",
    sparse=True,
    background=True,
)

TEXT_INDEX_DEFINITION = IndexModel(
    TEXT_INDEX_FIELDS,
    name=TEXT_INDEX_NAME,
    default_language="english",
    background=True,
)

GEO_LOC_INDEX = IndexModel(
    [("geo.loc", GEOSPHERE)],
    name="geo_loc_2dsphere_index",
    background=True,
    partialFilterExpression={
        "geo.loc.type": "Point",
        "geo.loc.coordinates.0": {"$type": "number"},
        "geo.loc.coordinates.1": {"$type": "number"},
    },
)

INDEXES = [
    DOMAIN_UNIQUE_INDEX,
    TEXT_INDEX_DEFINITION,
    A_RECORD_SPARSE_INDEX,
    GEO_LOC_INDEX,
]


def _normalize_index_key(key: Any) -> tuple:
    if isinstance(key, list):
        normalized = []
        for item in key:
            if isinstance(item, (list, tuple)):
                normalized.append(tuple(item))
            elif isinstance(item, Mapping):
                normalized.append(tuple(item.items())[0])
            else:
                normalized.append((item,))
        return tuple(normalized)
    if isinstance(key, Mapping):
        return tuple(key.items())
    return (key,)


def _normalize_weights(weights: Any) -> Any:
    if isinstance(weights, Mapping):
        return tuple(sorted(weights.items()))
    return weights


def _normalize_index_document(doc: Mapping[str, Any]) -> dict[str, Any]:
    ignored = {"ns", "v", "background", "textIndexVersion", "2dsphereIndexVersion"}
    normalized: dict[str, Any] = {}
    for key, value in doc.items():
        if key in ignored:
            continue
        if key == "key":
            normalized[key] = _normalize_index_key(value)
        elif key == "weights":
            normalized[key] = _normalize_weights(value)
        else:
            normalized[key] = value
    return normalized


def _index_matches(existing: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return _normalize_index_document(existing) == _normalize_index_document(expected)


async def recreate_text_index() -> None:
    """Re-create the full text index with the current field definition."""
    dns_collection = db.dns
    existing_indexes = {
        index["name"]: index
        for index in await dns_collection.list_indexes().to_list(length=None)
    }

    indexes_to_create = []

    for index_model in INDEXES:
        spec = index_model.document
        name = spec.get("name")
        existing = existing_indexes.get(name)

        if existing and _index_matches(existing, spec):
            continue

        if existing:
            try:
                await dns_collection.drop_index(name)
            except Exception:
                pass

        indexes_to_create.append(index_model)

    if indexes_to_create:
        await dns_collection.create_indexes(indexes_to_create)
