# Tools Reference

This directory contains one-off utilities that feed or enrich the MongoDB datasets used by the project. The scripts assume two databases on the target MongoDB host:

- `ip_data.dns` &mdash; core per-domain documents (A/AAAA records, metadata, enrichment markers).
- `ip_data.lookup` &mdash; auxiliary per-IP information (ports, WHOIS, etc.).
- `ip_data.asn` and `ip_data.ipv4` &mdash; supporting collections for ASN/IP inventories.
- `url_data.url` &mdash; harvested URLs collected during crawling.

All commands below expect Python 3.9+ and the dependencies listed in `tools/requirements.txt` (or the root `requirements.txt`). Install them inside a virtual environment before running any tool:

```bash
pip install -r tools/requirements.txt
```

Some scripts also depend on external binaries or services (for example `masscan`, `chromedriver`, or a MaxMind GeoIP database); those prerequisites are called out in their respective sections.

## Inventory At A Glance

| Script                      | Category      | Purpose                                                                          |
| --------------------------- | ------------- | -------------------------------------------------------------------------------- |
| `banner_grabber.py`         | Enrichment    | Capture SSH banners for domains with open port 22.                               |
| `crawl_urls.py`             | Crawling      | Fetch root pages for pending domains and store discovered links.                 |
| `cve_2019_19781_scanner.py` | Security      | Probe Citrix ADC appliances for the CVE-2019-19781 path traversal bug.           |
| `decode_idna.py`            | Normalisation | Convert punycode (`xn--`) hostnames in MongoDB back to Unicode.                  |
| `extract_certstream.py`     | Acquisition   | Subscribe to Certstream and ingest newly seen domains.                           |
| `extract_domains.py`        | Normalisation | Derive domain names from saved URLs.                                             |
| `extract_geoip.py`          | Enrichment    | Populate GeoIP fields by looking up A records in a MaxMind database.             |
| `extract_graph.py`          | Analysis      | Build a relationship graph between domains via DNS/SSL edges.                    |
| `extract_header.py`         | Enrichment    | Issue HTTP `HEAD` requests and store response headers.                           |
| `utils/extract_records.py`  | Enrichment    | Resolve common DNS record types for domains lacking data.                        |
| `extract_whois.py`          | Enrichment    | Fetch WHOIS/ASN details for `dns` or `ipv4` records.                             |
| `generate_qrcode.py`        | Reporting     | Generate base64 PNG QR codes for HTTPS URLs.                                     |
| `generate_sitemap.py`       | Reporting     | Merge Selenium-discovered URLs with an existing sitemap.                         |
| `import_domains.py`         | Ingestion     | Seed the URL collection from a plaintext list.                                   |
| `import_ip.py`              | Ingestion     | Insert IPv4 addresses (or CIDR ranges) into `ip_data.ipv4`.                      |
| `import_records.py`         | Ingestion     | Replay JSON lines with DNS answers into `ip_data.dns`.                           |
| `insert_asn.py`             | Ingestion     | Load AS numbers into `ip_data.asn`.                                              |
| `masscan_scanner.py`        | Security      | Run `masscan` against claimed IPs and persist open-port data.                    |
| `screenshot_scraper.py`     | Reporting     | Capture Chrome screenshots of HTTPS landing pages.                               |
| `ssl_cert_scanner.py`       | Enrichment    | Perform TLS handshakes, archive certificate metadata, and test protocol support. |

The remainder of this guide documents the behaviour, CLI flags, and workflow for each utility.

## Ingestion & Normalisation

### import_domains.py
- **Purpose:** Add raw URLs to `url_data.url`; useful as the first step before domain extraction.
- **CLI:** `python tools/import_domains.py --input urls.txt --host mongodb.internal`
- **Details:**
  - Lower-cases, strips `www.` prefixes, and inserts each URL with a `created` timestamp.
  - Enforces a unique index on `url`; duplicates are ignored.
- **Prerequisites:** Plaintext file with one URL per line.

### import_ip.py
- **Purpose:** Populate `ip_data.ipv4` with individual IPv4 addresses or expanded CIDR blocks.
- **CLI:** `python tools/import_ip.py --input ipv4.txt --host mongodb.internal`
- **Details:**
  - Creates a unique index on `ip` and inserts each address.
  - CIDR lines (e.g. `10.0.0.0/24`) are expanded into host IPs via the standard library `ipaddress` module.
- **Prerequisites:** Input file with one IP or CIDR per line.

### insert_asn.py
- **Purpose:** Seed the `ip_data.asn` collection with AS numbers for later enrichment.
- **CLI:** `python tools/insert_asn.py --input asn.txt --host mongodb.internal`
- **Details:** Stores each ASN along with an insertion timestamp; duplicates are skipped.

### import_records.py
- **Purpose:** Replay historical DNS observations (JSON lines) into MongoDB.
- **CLI:** `python tools/import_records.py --input records.jsonl --worker 4 --host mongodb.internal`
- **Details:**
  - Each line must contain fields like `query_name`, `resp_type`, and `data`.
  - Inserts or updates the matching domain document with `$addToSet` semantics per record type (`a_record`, `mx_record`, etc.).
  - Long-running worker processes mutate their shared `records` slice; re-run the script if it terminates early.
- **Prerequisites:** Structured JSONL export; ensure records are small enough for Mongo’s document size limit.

### decode_idna.py
- **Purpose:** Replace punycode hostnames (`xn--…`) with their decoded Unicode equivalents.
- **CLI:** `python tools/decode_idna.py --host mongodb.internal`
- **Details:**
  - Scans for domains matching the `xn--` regex, decodes with the `idna` library, and updates the `domain` field.
  - Writes an `updated` timestamp alongside the new value.

### extract_domains.py
- **Purpose:** Derive domain names from stored URLs and move them into `ip_data.dns`.
- **CLI:** `python tools/extract_domains.py --worker 4 --host mongodb.internal`
- **Details:**
  - Looks for URLs lacking `domain_extracted`, extracts the registrable domain (skipping raw IPv4s), and inserts into `ip_data.dns`.
  - Marks processed URLs with `domain_extracted` to prevent duplicate work.

## Crawling, Discovery & Reporting

### crawl_urls.py
- **Purpose:** Fetch `http://<domain>` for domains missing `domain_crawled` and store absolute links in `url_data.url`.
- **CLI:** `python tools/crawl_urls.py --worker 4 --host mongodb.internal --concurrency 25`
- **Details:**
  - Counts pending domains, splits them evenly across worker processes, and within each process runs `asyncio` tasks.
  - Extracts `<a href="…">` values via `lxml`, normalises relative URLs, and records them (ignoring duplicates).
  - Marks each domain with `domain_crawled` or `crawl_failed` depending on the outcome.
- **Tips:** Only plain HTTP is attempted; extend the script if HTTPS-only targets are expected. Watch the console for `[WARN]` messages.

### extract_certstream.py
- **Purpose:** Listen to the public Certstream feed and add observed hostnames to MongoDB.
- **CLI:** `python tools/extract_certstream.py --host mongodb.internal`
- **Details:**
  - Requires network access to `wss://certstream.calidog.io`.
  - On each certificate update event, strips leading `*.` from SAN entries and inserts every domain into `ip_data.dns` (unique on `domain`).
  - The callback opens a short-lived Mongo client per message; keep an eye on connection limits.

### generate_sitemap.py
- **Purpose:** Build a sitemap XML by visiting a seed page with Selenium and optionally merging with an existing file.
- **CLI:** `python tools/generate_sitemap.py --url https://app.example.com --input sitemap.xml`
- **Details:**
  - Launches headless Chromium via Selenium, scrapes all anchor `href`s, and filters for the `purplepee.co`/`api.purplepee.co` hosts.
  - If `--input` exists, previously listed URLs are merged before writing the XML back to disk.
- **Prerequisites:** Chrome/Chromium binary, compatible `chromedriver`, and a writable output file path.

### generate_qrcode.py
- **Purpose:** Store a base64-encoded PNG QR code that points to each HTTPS domain.
- **CLI:** `python tools/generate_qrcode.py --worker 4 --host mongodb.internal`
- **Details:**
  - Targets domains without a `qrcode` field; excludes punycode entries via regex.
  - Creates `https://<domain>` QR codes (`pyqrcode`) and saves them back to Mongo with an `updated` timestamp.

### screenshot_scraper.py
- **Purpose:** Capture full-page screenshots of HTTPS sites and attach the file name to each domain document.
- **CLI:** `python tools/screenshot_scraper.py --host mongodb.internal`
- **Details:**
  - Iterates over domains lacking `image`/`image_scan_failed`, renders them in headless Chromium, executes inline/external JavaScript, and saves PNGs into `screenshots/`.
  - Successful runs write the image filename and `updated` timestamp; failures get `image_scan_failed`.
- **Prerequisites:** Chromium + chromedriver, write access to `screenshots/`, and permissive network egress.

### extract_graph.py
- **Purpose:** Produce a graph of related domains (SSL SANs, CNAMEs, MX, NS) starting from a seed domain.
- **CLI:** `python tools/extract_graph.py --host mongodb.internal --domain example.com`
- **Details:** Uses MongoDB’s `$graphLookup` aggregation to discover relationships and prints a `nodes`/`edges` JSON structure suitable for visualisation.

## Enrichment Pipelines

### utils/extract_records.py
- **Purpose:** Resolve a standard set of DNS records (`A`, `AAAA`, `MX`, `NS`, `SOA`, `CNAME`) for domains lacking an `updated` timestamp.
- **CLI:** `python tools/utils/extract_records.py --worker 8 --host mongodb.internal`
- **Details:**
  - Splits the domain list across worker processes, performs synchronous `dns.resolver` lookups (1 second timeout), and stores results with `$addToSet` semantics.
  - Marks domains with `update_failed` timestamps when no records are found.

### banner_grabber.py
- **Purpose:** Fetch SSH banners from IPs referenced by domain A records.
- **CLI:** `python tools/banner_grabber.py --worker 8 --host mongodb.internal --batch 100`
- **Details:**
  - Looks for domains that list port 22 in `ports.port` but lack `banner`/`banner_scan_failed` markers.
  - Claims batches by setting `in_progress`, attempts a socket connect/read, and stores the banner or a failure timestamp.

### extract_header.py
- **Purpose:** Collect HTTP response headers via async `HEAD` requests.
- **CLI:** `python tools/extract_header.py --host mongodb.internal --workers 4`
- **Details:**
  - Each process grabs pending domains (with open ports 80/443) and fires up to `CONCURRENCY` concurrent requests using `httpx` + `fake_useragent`.
  - Persists headers, HTTP status/version, and `updated` timestamp; failures get `header_scan_failed`.

### extract_geoip.py
- **Purpose:** Enrich A-record domains with GeoIP data from a local MaxMind database.
- **CLI:** `python tools/extract_geoip.py --input /path/GeoLite2-City.mmdb --host mongodb.internal --workers 6 --chunk-size 2000`
- **Details:**
  - Counts domains lacking `country_code`, splits them across worker processes, and for each IP writes location fields under both legacy (`country`, `state`, etc.) and nested `geo` keys.
  - Records failures via `geo_lookup_failed`.
- **Prerequisites:** A GeoIP2/City `.mmdb` file (GeoLite2 or commercial) accessible on disk.

### extract_whois.py
- **Purpose:** Retrieve ASN/WHOIS data for either domain (`dns`) or raw IP (`ipv4`) records.
- **CLI:** `python tools/extract_whois.py --collection dns --worker 4 --host mongodb.internal`
- **Details:**
  - Workers claim documents missing `whois.asn`, query the `ipwhois` library, and store the response plus an `updated` timestamp.
  - For the `ipv4` collection, WHOIS results are written to `ip_data.lookup` and the `subnet` flag is cleared.

### ssl_cert_scanner.py
- **Purpose:** Perform TLS handshakes against port 443, archive certificate metadata, and record which TLS protocol versions are accepted.
- **CLI:** `python tools/ssl_cert_scanner.py --host mongodb.internal --workers 4`
- **Details:**
  - Each process claims pending domains (must list port 443), extracts issuer/subject/SAN fields, handshake details, and tests TLS 1.0–1.3 individually.
  - Successful runs populate `ssl`, failed handshakes write `ssl_scan_failed`.

### masscan_scanner.py
- **Purpose:** Run the `masscan` binary against batches of claimed IPs and save any open ports.
- **CLI:** `python tools/masscan_scanner.py --workers 4 --input ports.txt --host mongodb.internal --batch 500 --rate 2000`
- **Details:**
  - Requires the `masscan` executable and appropriate privileges (often `CAP_NET_RAW`).
  - Batches IPs lacking `ports`, runs `masscan -oJ`, parses the JSON, and updates both `ip_data.lookup` and `ip_data.dns`.
  - Marks documents as `claimed` while scanning to avoid duplication.

## Security & Monitoring

### cve_2019_19781_scanner.py
- **Purpose:** Check Citrix ADC/NetScaler appliances for the CVE-2019-19781 directory traversal.
- **CLI:** `python tools/cve_2019_19781_scanner.py 192.0.2.0/24 443`
- **Details:**
  - Expands the target CIDR into IPv4s, spawns multiple processes, and requests `/vpn/../vpns/cfg/smb.conf`.
  - Prints coloured output when the response hints at vulnerability. (Note: the current implementation hardcodes port `443` when launching workers.)
- **Prerequisites:** Open network path to the targets; run from a network vantage point that mimics attacker traffic.

## Typical End-to-End Workflow

1. **Seed data:** Run `import_domains.py`, `import_ip.py`, and `insert_asn.py` as needed to populate MongoDB.
2. **Derive domains:** Execute `extract_domains.py` to turn stored URLs into registrable domains.
3. **Resolve DNS:** Use `utils/extract_records.py` (and optionally `import_records.py`) to enrich each domain with DNS answers.
4. **Network enrichment:** Launch `extract_geoip.py`, `extract_header.py`, `ssl_cert_scanner.py`, `extract_whois.py`, `banner_grabber.py`, and `masscan_scanner.py` to gather network metadata.
5. **Crawling & reporting:** Run `crawl_urls.py`, `generate_qrcode.py`, `screenshot_scraper.py`, and `generate_sitemap.py` for additional context and presentation outputs.
6. **Continuous discovery:** Keep `extract_certstream.py` (and optional security checks like `cve_2019_19781_scanner.py`) running to ingest newly observed assets.

All scripts are idempotent: they rely on marker fields (`*_failed`, `claimed`, `updated`, etc.) so that interrupting and re-running them is safe. Validate new configurations with a small worker count before scaling out.
