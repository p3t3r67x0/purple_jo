# Purplepee Backend

API backend for the Purplepee open-source ASN lookup project. The service is
implemented with **FastAPI** and served with **Uvicorn**.


## Database Setup

```
sudo apt-get install gnupg
wget --quiet -O - https://www.mongodb.org/static/pgp/server-6.0.asc | gpg --dearmor > mongodb-keyring.gpg
sudo mv mongodb-keyring.gpg /etc/apt/trusted.gpg.d/
sudo chown root:root /etc/apt/trusted.gpg.d/mongodb-keyring.gpg
sudo chmod ugo+r /etc/apt/trusted.gpg.d/mongodb-keyring.gpg
sudo chmod go-w /etc/apt/trusted.gpg.d/mongodb-keyring.gpg
echo "deb [arch=amd64,arm64 signed-by=/etc/apt/trusted.gpg.d/mongodb-keyring.gpg] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list
sudo apt update
sudo apt install mongodb-org
```


When you are done installing MongoDB, make sure it is running locally and the
connection strings in `config.cfg` (or your environment variables) point to the
correct instance.


## Application Setup

The backend now runs on FastAPI; Flask is no longer used. Use Python 3.10+.

```bash
python -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Create a `.env` file or export the relevant environment variables if you do not
want to rely on `config.cfg`.


## Running the API

Development server with auto-reload:

```bash
uvicorn app.main:app --reload
```

Production-style startup (example):

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

Interactive API documentation is available once the server is running:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`


## Request Trends

The API exposes aggregated request statistics via `GET /trends/requests`. You can
control the time window and aggregation interval using the query parameters:

```text
/trends/requests?interval=minute&lookback_minutes=120&buckets=30&top_paths=5
```

The response includes a timeline of request counts, the most frequently used
paths, and a short list of the most recent calls so that you can quickly spot
usage patterns.


Example output of `curl https://api.purplepee.co/match/asn:46279` entries

```json
[{
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "stonemillpetresort.com"
	}],
	"created": "Sat, 07 Dec 2019 00:08:16 GMT",
	"domain": "www.stonemillpetresort.com",
	"updated": "Thu, 09 Jan 2020 10:23:06 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "111westemporiumtn.com"
	}],
	"created": "Mon, 16 Dec 2019 16:10:17 GMT",
	"domain": "www.111westemporiumtn.com",
	"ns_record": ["ns29.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 10:20:29 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61", "173.201.73.46"],
	"created": "Mon, 16 Dec 2019 16:00:31 GMT",
	"domain": "www.vandlplumbinginc.com",
	"ns_record": ["j.gtld-servers.net", "g.gtld-servers.net", "f.gtld-servers.net", "c.gtld-servers.net", "e.gtld-servers.net", "a.gtld-servers.net", "ns72.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 10:18:46 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.101"],
	"cname_record": [{
		"target": "techproinc.com"
	}],
	"created": "Mon, 16 Dec 2019 16:17:06 GMT",
	"domain": "www.techproinc.com",
	"ns_record": ["a.gtld-servers.net", "h.gtld-servers.net", "g.gtld-servers.net", "f.gtld-servers.net", "i.gtld-servers.net", "e.gtld-servers.net"],
	"updated": "Thu, 09 Jan 2020 10:16:41 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61", "198.41.0.4"],
	"created": "Tue, 10 Dec 2019 00:01:09 GMT",
	"domain": "www.usedcarphoenixaz.com",
	"updated": "Thu, 09 Jan 2020 10:14:22 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61", "192.31.80.30", "192.55.83.30"],
	"created": "Fri, 13 Dec 2019 03:40:36 GMT",
	"domain": "www.lichtenbergerhomesreviews.com",
	"updated": "Thu, 09 Jan 2020 10:13:51 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "thegalleryshowroom.com"
	}],
	"created": "Sat, 07 Dec 2019 18:10:27 GMT",
	"domain": "www.thegalleryshowroom.com",
	"ns_record": ["ns49.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 10:12:47 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61", "173.201.77.20", "97.74.109.20"],
	"aaaa_record": ["2603:5:22d1::14"],
	"cname_record": [{
		"target": "whitewaterglasscompanyinc.com"
	}],
	"created": "Mon, 09 Dec 2019 15:06:21 GMT",
	"domain": "www.whitewaterglasscompanyinc.com",
	"ns_record": ["ns40.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 10:12:09 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61", "97.74.105.26", "173.201.73.26"],
	"cname_record": [{
		"target": "alwaysawomania.com"
	}],
	"created": "Fri, 13 Dec 2019 03:38:11 GMT",
	"domain": "www.alwaysawomania.com",
	"ns_record": ["ns51.domaincontrol.com", "ns52.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 10:09:10 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "plushboutiquecharleston.com"
	}],
	"created": "Sat, 07 Dec 2019 17:49:08 GMT",
	"domain": "www.plushboutiquecharleston.com",
	"updated": "Thu, 09 Jan 2020 10:09:08 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "fontzgraphicsreviews.com"
	}],
	"created": "Mon, 16 Dec 2019 16:23:01 GMT",
	"domain": "www.fontzgraphicsreviews.com",
	"updated": "Thu, 09 Jan 2020 10:01:17 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"aaaa_record": ["2603:5:2181::f", "2603:5:2281::f"],
	"cname_record": [{
		"target": "securitybuildingservicesil.com"
	}],
	"created": "Thu, 05 Dec 2019 14:58:05 GMT",
	"domain": "www.securitybuildingservicesil.com",
	"ns_record": ["ns30.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 10:00:30 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "foxvalleyenv.com"
	}],
	"created": "Fri, 13 Dec 2019 03:42:05 GMT",
	"domain": "www.foxvalleyenv.com",
	"updated": "Thu, 09 Jan 2020 09:58:16 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "medicappharmacyglenwood.com"
	}],
	"created": "Mon, 16 Dec 2019 16:15:51 GMT",
	"domain": "www.medicappharmacyglenwood.com",
	"updated": "Thu, 09 Jan 2020 09:55:22 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"aaaa_record": ["2603:5:2174::2c", "2603:5:2274::2c"],
	"created": "Mon, 16 Dec 2019 16:25:04 GMT",
	"domain": "www.countrysideoutdoorpower.com",
	"ns_record": ["ns68.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 09:51:44 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "lawnserviceknox.com"
	}],
	"created": "Thu, 05 Dec 2019 16:32:42 GMT",
	"domain": "www.lawnserviceknox.com",
	"header_scan_failed": "Sat, 07 Dec 2019 22:21:47 GMT",
	"updated": "Thu, 09 Jan 2020 09:51:02 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.155", "50.235.192.10"],
	"cname_record": [{
		"target": "randaldisplays.com"
	}],
	"created": "Mon, 09 Dec 2019 16:39:00 GMT",
	"domain": "www.randaldisplays.com",
	"ns_record": ["ns2.techpro.com"],
	"updated": "Thu, 09 Jan 2020 09:50:09 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61", "173.201.74.7"],
	"aaaa_record": ["2603:5:22a0::7"],
	"cname_record": [{
		"target": "horsebackridingshiloranch.com"
	}],
	"created": "Sat, 07 Dec 2019 14:57:25 GMT",
	"domain": "www.horsebackridingshiloranch.com",
	"mx_record": [{
		"exchange": "mailstore1.secureserver.net",
		"preference": 10
	}, {
		"exchange": "smtp.secureserver.net",
		"preference": 0
	}],
	"ns_record": ["ns14.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 09:43:33 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"aaaa_record": ["2603:5:2194::2e", "2603:5:2294::2e"],
	"cname_record": [{
		"target": "familytreechildcarecenterwi.com"
	}],
	"created": "Fri, 13 Dec 2019 03:37:07 GMT",
	"domain": "www.familytreechildcarecenterwi.com",
	"ns_record": ["ns72.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 09:43:03 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"aaaa_record": ["2603:5:22d0::a", "2603:5:21d0::a"],
	"cname_record": [{
		"target": "megahomerealtyil.com"
	}],
	"created": "Fri, 13 Dec 2019 03:48:26 GMT",
	"domain": "www.megahomerealtyil.com",
	"ns_record": ["ns20.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 09:38:33 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "johnsonandjohnsonattorneys.com"
	}],
	"created": "Fri, 13 Dec 2019 03:46:21 GMT",
	"domain": "www.johnsonandjohnsonattorneys.com",
	"ns_record": ["a.gtld-servers.net", "h.gtld-servers.net", "k.gtld-servers.net", "d.gtld-servers.net", "b.gtld-servers.net", "m.gtld-servers.net"],
	"updated": "Thu, 09 Jan 2020 09:37:33 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61", "192.5.6.30", "192.26.92.30", "192.12.94.30", "192.42.93.30", "192.43.172.30"],
	"created": "Mon, 16 Dec 2019 16:24:00 GMT",
	"domain": "www.cellardoormayfieldky.com",
	"updated": "Thu, 09 Jan 2020 09:34:40 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "rinksdetailing.com"
	}],
	"created": "Thu, 05 Dec 2019 09:55:04 GMT",
	"domain": "www.rinksdetailing.com",
	"updated": "Thu, 09 Jan 2020 09:29:44 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"aaaa_record": ["2603:5:2282::19", "2603:5:2182::19"],
	"created": "Mon, 16 Dec 2019 15:59:39 GMT",
	"domain": "www.longtreeservice.com",
	"ns_record": ["ns50.domaincontrol.com", "ns49.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 09:29:40 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61", "192.5.6.30", "192.33.14.30", "192.26.92.30", "192.31.80.30", "192.12.94.30"],
	"created": "Mon, 16 Dec 2019 16:02:55 GMT",
	"domain": "www.graystoneconstructioncompanyinc.com",
	"updated": "Thu, 09 Jan 2020 09:29:07 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.101"],
	"cname_record": [{
		"target": "puls-us.com"
	}],
	"created": "Fri, 13 Dec 2019 03:47:25 GMT",
	"domain": "www.puls-us.com",
	"updated": "Thu, 09 Jan 2020 09:28:01 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "bigrivercaninecoachia.com"
	}],
	"created": "Thu, 19 Dec 2019 23:30:46 GMT",
	"domain": "www.bigrivercaninecoachia.com",
	"ns_record": ["ns61.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 09:15:05 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61"],
	"cname_record": [{
		"target": "tpcabinetryreviews.com"
	}],
	"created": "Mon, 09 Dec 2019 04:07:28 GMT",
	"domain": "www.tpcabinetryreviews.com",
	"updated": "Thu, 09 Jan 2020 09:14:28 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61", "97.74.101.12", "173.201.69.12"],
	"cname_record": [{
		"target": "sandcgutters.com"
	}],
	"created": "Thu, 05 Dec 2019 16:49:58 GMT",
	"domain": "www.sandcgutters.com",
	"ns_record": ["ns24.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 09:13:38 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}, {
	"a_record": ["208.93.159.61", "97.74.100.11"],
	"aaaa_record": ["2603:5:2141::b", "2603:5:2241::b"],
	"cname_record": [{
		"target": "roanokeplumbingheatingandcooling.com"
	}],
	"created": "Thu, 05 Dec 2019 11:56:10 GMT",
	"domain": "www.roanokeplumbingheatingandcooling.com",
	"ns_record": ["ns21.domaincontrol.com"],
	"updated": "Thu, 09 Jan 2020 09:08:22 GMT",
	"whois": {
		"asn": "46279",
		"asn_cidr": "208.93.159.0/24",
		"asn_country_code": "US",
		"asn_date": "2008-08-06",
		"asn_description": "TECHPRO-01 - TechPro, Inc, US",
		"asn_registry": "arin"
	}
}]
```


## Build Setup

```bash
# install build/runtime dependencies you might need
sudo apt install redis-server chromium-chromedriver python3 python3-dev gcc

# create a virtualenv
python3 -m venv venv

# activate virtualenv
. venv/bin/activate

# install python dependencies
pip install --upgrade pip
pip install -r requirements.txt

# optional: run the API locally (bound to localhost:8000)
uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 2 --log-level info
```


## Systemd Setup

Create a file `/etc/systemd/system/purplejo.service` with following content

```bash
[Unit]
Description=Uvicorn instance to serve purplejo
After=network.target

[Service]
User=<user>
Group=www-data
WorkingDirectory=/home/<user>/git/purple_jo
Environment="PATH=/home/<user>/git/purple_jo/venv/bin"
ExecStart=/home/<user>/git/purple_jo/venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 4 --log-level info --proxy-headers
Restart=on-failure
RestartSec=2s

[Install]
WantedBy=multi-user.target
```


Start the service and enable the service

```bash
sudo systemctl start purplejo
sudo systemctl enable purplejo
```


## Setup Nginx with SSL

Install dependencies from Ubuntu repository

```bash
sudo apt install nginx-full certbot python-certbot-nginx
```


Setup nginx config file in `/etc/nginx/sites-enabled/api_example_com`

```cfg
server {
    server_name api.example.com;

    location / {
        if ($request_method = 'OPTIONS') {
            add_header 'Access-Control-Allow-Origin' 'https://example.com' always;
            add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;

            # Custom headers and headers various browsers *should* be OK with but aren't
            add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;

            # Tell client that this pre-flight info is valid for 20 days
            add_header 'Access-Control-Max-Age' 1728000 always;
            add_header 'Content-Type' 'text/plain; charset=utf-8' always;
            add_header 'Content-Length' 0 always;
            return 204;
        }

        if ($request_method = 'POST') {
            add_header 'Access-Control-Allow-Origin' 'https://example.com' always;
            add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
            add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;
        }

        if ($request_method = 'GET') {
            add_header 'Access-Control-Allow-Origin' 'https://example.com' always;
            add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
            add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;
        }

        if ($request_method = 'PUT') {
            add_header 'Access-Control-Allow-Origin' 'https://example.com' always;
            add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
            add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;
        }

        if ($request_method = 'DELETE') {
            add_header 'Access-Control-Allow-Origin' 'https://example.com' always;
            add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
            add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;
        }

        proxy_pass http://127.0.0.1:8000/;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Host $http_host;
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_set_header X-Forwarded-Port $server_port;
    }
}
```
