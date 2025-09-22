from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import IndexModel, TEXT

from app.config import DB_NAME, MONGO_URI


mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]


LEGACY_TEXT_INDEXES = ("domain_text_header.x-powered-by_text",)

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
    ("geo.loc.coordinates", TEXT),
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

INDEXES = [DOMAIN_UNIQUE_INDEX, TEXT_INDEX_DEFINITION, A_RECORD_SPARSE_INDEX]


async def recreate_text_index() -> None:
    """Re-create the full text index with the current field definition."""
    for index_name in (*LEGACY_TEXT_INDEXES, TEXT_INDEX_NAME):
        try:
            await db.dns.drop_index(index_name)
        except Exception:
            # Index did not exist or could not be dropped; continue regardless.
            pass

    await db.dns.create_indexes(INDEXES)
