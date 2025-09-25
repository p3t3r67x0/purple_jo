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
    [("domain", 1), ("updated", -1)],
    name="domain_unique_index",
    sparse=True,
    unique=True,
    background=True,
)

A_RECORD_SPARSE_INDEX = IndexModel(
    [("a_record", 1), ("updated", -1)],
    name="a_record_updated_index",
    sparse=True,
    background=True,
)

AAAA_RECORD_SPARSE_INDEX = IndexModel(
    [("aaaa_record", 1), ("updated", -1)],
    name="aaaa_record_updated_index",
    sparse=True,
    background=True,
)

UPDATED_DESC_INDEX = IndexModel(
    [("updated", -1)],
    name="updated_desc_index",
    background=True
)

WHOIS_ASN_INDEX = IndexModel(
    [("whois.asn", 1), ("updated", -1)],
    name="whois_asn_updated_index",
    sparse=True,
    background=True,
)

WHOIS_ASN_CIDR_INDEX = IndexModel(
    [("whois.asn_cidr", 1), ("updated", -1)],
    name="whois_asn_cidr_updated_index",
    sparse=True,
    background=True,
)

WHOIS_ASN_REGISTRY_INDEX = IndexModel(
    [("whois.asn_registry", 1), ("updated", -1)],
    name="whois_asn_registry_updated_index",
    sparse=True,
    background=True,
)

WHOIS_ASN_COUNTRY_INDEX = IndexModel(
    [("whois.asn_country_code", 1), ("updated", -1)],
    name="whois_asn_country_updated_index",
    sparse=True,
    background=True,
)

GEO_COUNTRY_CODE_INDEX = IndexModel(
    [("geo.country_code", 1), ("updated", -1)],
    name="geo_country_code_updated_index",
    sparse=True,
    background=True,
)

GEO_STATE_INDEX = IndexModel(
    [("geo.state", 1), ("updated", -1)],
    name="geo_state_updated_index",
    sparse=True,
    background=True,
)

GEO_CITY_INDEX = IndexModel(
    [("geo.city", 1), ("updated", -1)],
    name="geo_city_updated_index",
    sparse=True,
    background=True,
)

PORTS_PORT_INDEX = IndexModel(
    [("ports.port", 1), ("updated", -1)],
    name="ports_port_updated_index",
    sparse=True,
    background=True,
)

MX_EXCHANGE_INDEX = IndexModel(
    [("mx_record.exchange", 1), ("updated", -1)],
    name="mx_exchange_updated_index",
    sparse=True,
    background=True,
)

NS_RECORD_INDEX = IndexModel(
    [("ns_record", 1), ("updated", -1)],
    name="ns_record_updated_index",
    sparse=True,
    background=True,
)

CNAME_TARGET_INDEX = IndexModel(
    [("cname_record.target", 1), ("updated", -1)],
    name="cname_target_updated_index",
    sparse=True,
    background=True,
)

HEADER_STATUS_INDEX = IndexModel(
    [("header.status", 1), ("updated", -1)],
    name="header_status_updated_index",
    sparse=True,
    background=True,
)

HEADER_POWERED_BY_INDEX = IndexModel(
    [("header.x-powered-by", 1), ("updated", -1)],
    name="header_powered_by_updated_index",
    sparse=True,
    background=True,
)

BANNER_UPDATED_INDEX = IndexModel(
    [("banner", 1), ("updated", -1)],
    name="banner_updated_index",
    sparse=True,
    background=True,
)

HEADER_SERVER_INDEX = IndexModel(
    [("header.server", 1), ("updated", -1)],
    name="header_server_updated_index",
    sparse=True,
    background=True,
)

SSL_SUBJECT_CN_INDEX = IndexModel(
    [("ssl.subject.common_name", 1), ("updated", -1)],
    name="ssl_subject_common_name_index",
    sparse=True,
    background=True,
)

SSL_SUBJECT_ALT_NAMES_INDEX = IndexModel(
    [("ssl.subject_alt_names", 1), ("updated", -1)],
    name="ssl_subject_alt_names_index",
    sparse=True,
    background=True,
)

SSL_SUBJECT_ORG_INDEX = IndexModel(
    [("ssl.subject.organization_name", 1), ("updated", -1)],
    name="ssl_subject_organization_name_index",
    sparse=True,
    background=True,
)

SSL_ISSUER_ORG_INDEX = IndexModel(
    [("ssl.issuer.organization_name", 1), ("updated", -1)],
    name="ssl_issuer_org_index",
    sparse=True,
    background=True,
)

SSL_ISSUER_CN_INDEX = IndexModel(
    [("ssl.issuer.common_name", 1), ("updated", -1)],
    name="ssl_issuer_common_name_index",
    sparse=True,
    background=True,
)

SSL_SUBJECT_OU_INDEX = IndexModel(
    [("ssl.subject.organizational_unit_name", 1), ("updated", -1)],
    name="ssl_subject_ou_index",
    sparse=True,
    background=True,
)

SSL_ISSUER_OU_INDEX = IndexModel(
    [("ssl.issuer.organizational_unit_name", 1), ("updated", -1)],
    name="ssl_issuer_ou_index",
    sparse=True,
    background=True,
)

SSL_CA_ISSUERS_INDEX = IndexModel(
    [("ssl.ca_issuers", 1), ("updated", -1)],
    name="ssl_ca_issuers_index",
    sparse=True,
    background=True,
)

SSL_OCSP_INDEX = IndexModel(
    [("ssl.ocsp", 1), ("updated", -1)],
    name="ssl_ocsp_index",
    sparse=True,
    background=True,
)

SSL_CRL_INDEX = IndexModel(
    [("ssl.crl_distribution_points", 1), ("updated", -1)],
    name="ssl_crl_distribution_points_index",
    sparse=True,
    background=True,
)

SSL_NOT_BEFORE_INDEX = IndexModel(
    [("ssl.not_before", -1), ("updated", -1)],
    name="ssl_not_before_index",
    sparse=True,
    background=True,
)

SSL_NOT_AFTER_INDEX = IndexModel(
    [("ssl.not_after", -1), ("updated", -1)],
    name="ssl_not_after_index",
    sparse=True,
    background=True,
)

WHOIS_ASN_DESCRIPTION_INDEX = IndexModel(
    [("whois.asn_description", 1), ("updated", -1)],
    name="whois_asn_description_updated_index",
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
    AAAA_RECORD_SPARSE_INDEX,
    GEO_LOC_INDEX,
    UPDATED_DESC_INDEX,
    WHOIS_ASN_INDEX,
    WHOIS_ASN_DESCRIPTION_INDEX,
    WHOIS_ASN_CIDR_INDEX,
    WHOIS_ASN_REGISTRY_INDEX,
    WHOIS_ASN_COUNTRY_INDEX,
    GEO_COUNTRY_CODE_INDEX,
    GEO_STATE_INDEX,
    GEO_CITY_INDEX,
    PORTS_PORT_INDEX,
    MX_EXCHANGE_INDEX,
    NS_RECORD_INDEX,
    CNAME_TARGET_INDEX,
    HEADER_STATUS_INDEX,
    HEADER_POWERED_BY_INDEX,
    HEADER_SERVER_INDEX,
    BANNER_UPDATED_INDEX,
    SSL_SUBJECT_CN_INDEX,
    SSL_SUBJECT_ORG_INDEX,
    SSL_SUBJECT_ALT_NAMES_INDEX,
    SSL_ISSUER_ORG_INDEX,
    SSL_ISSUER_CN_INDEX,
    SSL_SUBJECT_OU_INDEX,
    SSL_ISSUER_OU_INDEX,
    SSL_CA_ISSUERS_INDEX,
    SSL_OCSP_INDEX,
    SSL_CRL_INDEX,
    SSL_NOT_BEFORE_INDEX,
    SSL_NOT_AFTER_INDEX,
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
    ignored = {"ns", "v", "background",
               "textIndexVersion", "2dsphereIndexVersion"}
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
