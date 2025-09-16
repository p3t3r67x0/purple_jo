from fastapi import FastAPI, Depends
from motor.motor_asyncio import AsyncIOMotorClient
from app.services import http, dns, ssl, geoip, masscan, whois, banners, qrcode
from app.db import get_db

app = FastAPI()


@app.get("/headers/{domain}")
async def headers(domain: str):
    return await http.fetch_site_headers(domain)


@app.get("/dns/{domain}")
async def dns_records(domain: str):
    return await dns.fetch_dns_records(domain)


@app.get("/ssl/{domain}")
async def ssl_cert(domain: str):
    return await ssl.extract_certificate(domain)


@app.get("/whois/{ip}")
async def whois_lookup(ip: str):
    return await whois.fetch_asn_whois(ip)
