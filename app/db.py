from motor.motor_asyncio import AsyncIOMotorClient
from app.config import MONGO_URI, DB_NAME

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]


async def recreate_text_index():
    try:
        await db.dns.drop_index("domain_text_header.x-powered-by_text")
    except Exception:
        pass

    await db.dns.create_index(
        [
            ("domain", "text"),
            ("header.x-powered-by", "text"),
            ("banner", "text"),
            ("ports.port", "text"),
            ("whois.asn", "text"),
            ("whois.asn_description", "text"),
            ("whois.asn_country_code", "text"),
            ("whois.asn_registry", "text"),
            ("whois.asn_cidr", "text"),
            ("cname_record.target", "text"),
            ("mx_record.exchange", "text"),
            ("header.server", "text"),
            ("header.status", "text"),
            ("ns_record", "text"),
            ("aaaa_record", "text"),
            ("a_record", "text"),
            ("geo.loc.coordinates", "text"),
            ("geo.country_code", "text"),
            ("geo.country", "text"),
            ("geo.state", "text"),
            ("geo.city", "text"),
            ("ssl.ocsp", "text"),
            ("ssl.not_after", "text"),
            ("ssl.not_before", "text"),
            ("ssl.ca_issuers", "text"),
            ("ssl.issuer.common_name", "text"),
            ("ssl.issuer.organization_name", "text"),
            ("ssl.issuer.organizational_unit_name", "text"),
            ("ssl.subject_alt_names", "text"),
            ("ssl.subject.common_name", "text"),
            ("ssl.subject.organizational_unit_name", "text"),
            ("ssl.subject.organization_name", "text"),
            ("ssl.crl_distribution_points", "text"),
        ],
        name="all_fields_text_index",
        default_language="english",
        background=True,
    )
