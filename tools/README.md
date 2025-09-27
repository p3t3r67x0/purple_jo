# Tools Reference

This directory contains one-off utilities that feed or enrich the PostgreSQL database used by the project. The scripts interact with a PostgreSQL database containing structured tables for domains, DNS records, GeoIP data, SSL certificates, and other network intelligence.

The database schema includes the following main components:
- **Domains** &mdash; core domain records with A/AAAA records, metadata, and enrichment markers
- **DNS Records** &mdash; structured A, AAAA, MX, NS, SOA, and CNAME records
- **GeoIP Data** &mdash; geographical location information for IP addresses
- **SSL Data** &mdash; certificate metadata and TLS configuration details
- **WHOIS Records** &mdash; ASN and registration information
- **Port Services** &mdash; open port and service detection results

All commands below expect Python 3.9+ and the dependencies listed in `tools/requirements.txt`. Install them inside a virtual environment before running any tool:

```bash
pip install -r tools/requirements.txt
```

Most tools require a PostgreSQL connection string via the `POSTGRES_DSN` environment variable or `.env` file:

```bash
export POSTGRES_DSN="postgresql+asyncpg://username:password@localhost/purple_jo"
```

**Database Requirements:**
- PostgreSQL 12+ with async support via asyncpg driver
- Database schema initialized via Alembic migrations (`alembic upgrade head`)
- Proper indexes and constraints are automatically created by the migration system
- Connection pooling is configured per tool for optimal performance

Some scripts also depend on external binaries or services (for example `masscan`, `chromedriver`, or a MaxMind GeoIP database); those prerequisites are called out in their respective sections.

## Inventory At A Glance

| Script                      | Category      | Purpose                                                                          |
| --------------------------- | ------------- | -------------------------------------------------------------------------------- |
| `banner_grabber.py`         | Enrichment    | Capture SSH banners for domains with open port 22.                               |
| `crawl_urls.py`             | Crawling      | Fetch root pages for pending domains and store discovered links.                 |
| `cve_2019_19781_scanner.py` | Security      | Probe Citrix ADC appliances for the CVE-2019-19781 path traversal bug.           |
| `decode_idna.py`            | Normalisation | Convert punycode (`xn--`) hostnames in PostgreSQL back to Unicode.               |
| `extract_certstream.py`     | Acquisition   | Subscribe to Certstream and ingest newly seen domains.                           |
| `extract_domains.py`        | Normalisation | Derive domain names from saved URLs.                                             |
| `extract_geoip.py`          | Enrichment    | Populate GeoIP fields by looking up A records in a MaxMind database (PostgreSQL). |
| `extract_graph.py`          | Analysis      | Build a relationship graph between domains via DNS/SSL edges.                    |
| `extract_header.py`         | Enrichment    | Issue HTTP `HEAD` requests and store response headers.                           |
| `extract_records.py`        | Enrichment    | Resolve common DNS record types for domains lacking data.                        |
| `extract_whois.py`          | Enrichment    | Fetch WHOIS/ASN details for `dns` or `ipv4` records.                             |
| `generate_qrcode.py`        | Reporting     | Generate base64 PNG QR codes for HTTPS URLs.                                     |
| `generate_sitemap.py`       | Reporting     | Merge Selenium-discovered URLs with an existing sitemap.                         |
| `import_domains.py`         | Ingestion     | Seed the URL collection from a plaintext list.                                   |
| `import_ip.py`              | Ingestion     | Insert IPv4 addresses (or CIDR ranges) into PostgreSQL subnet tables.           |
| `import_records.py`         | Ingestion     | Replay JSON lines with DNS answers into PostgreSQL DNS tables.                   |
| `insert_asn.py`             | Ingestion     | Load AS numbers into PostgreSQL ASN tables.                                      |
| `masscan_scanner.py`        | Security      | Run `masscan` against claimed IPs and persist open-port data.                    |
| `screenshot_scraper.py`     | Reporting     | Capture Chrome screenshots of HTTPS landing pages.                               |
| `ssl_cert_scanner.py`       | Enrichment    | Perform TLS handshakes, archive certificate metadata, and test protocol support. |

The remainder of this guide documents the behaviour, CLI flags, and workflow for each utility.

## Ingestion & Normalisation

### import_domains.py
- **Purpose:** Add raw URLs to the `urls` table; useful as the first step before domain extraction.
- **CLI:** `python tools/import_domains.py --input urls.txt --postgres-dsn "postgresql+asyncpg://..."`
- **Details:**
  - Lower-cases, strips `www.` prefixes, and inserts each URL with a `created_at` timestamp.
  - Enforces a unique constraint on `url`; duplicates are ignored.
- **Prerequisites:** Plaintext file with one URL per line.

### import_ip.py
- **Purpose:** Populate the `subnet_lookups` table with individual IPv4 addresses or expanded CIDR blocks.
- **CLI:** `python tools/import_ip.py --input ipv4.txt --postgres-dsn "postgresql+asyncpg://..."`
- **Details:**
  - Creates entries in the subnet lookup table and inserts each address.
  - CIDR lines (e.g. `10.0.0.0/24`) are expanded into host IPs via the standard library `ipaddress` module.
- **Prerequisites:** Input file with one IP or CIDR per line.

### insert_asn.py
- **Purpose:** Seed the ASN-related tables with AS numbers for later enrichment.
- **CLI:** `python tools/insert_asn.py --input asn.txt --postgres-dsn "postgresql+asyncpg://..."`
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
- **CLI:** `python tools/decode_idna.py --postgres-dsn "postgresql+asyncpg://..."`
- **Details:**
  - Scans domains table for entries matching the `xn--` regex, decodes with the `idna` library, and updates the domain name.
  - Updates the `updated_at` timestamp alongside the new value.

### extract_domains.py
- **Purpose:** Derive domain names from stored URLs and create entries in the domains table.
- **CLI:** `python tools/extract_domains.py --worker 4 --postgres-dsn "postgresql+asyncpg://..."`
- **Details:**
  - Looks for URLs lacking `domain_extracted`, extracts the registerable domain (skipping raw IPv4s), and inserts into the domains table.
  - Marks processed URLs with `domain_extracted` to prevent duplicate work.

## Crawling, Discovery & Reporting

### crawl_urls.py
- **Purpose:** Crawl pending domains, following redirects where necessary, persist discovered links, and log crawl outcomes (including the final URL) to PostgreSQL and the console.
- **CLI:**
  - Queue RabbitMQ jobs from PostgreSQL: `python tools/crawl_urls.py --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --queue-name crawl_domains --purge-queue`
  - Run long-lived workers: `python tools/crawl_urls.py --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --service --worker 4 --concurrency 250 --log-urls`
  - Direct/batch mode (no RabbitMQ): `python tools/crawl_urls.py --postgres-dsn "postgresql+asyncpg://..." --worker 4 --concurrency 250`
- **Details:**
  - Publishes `STOP` sentinels after queueing so multiple service instances can exit cleanly once work is drained.
  - Workers share an async HTTP client per process, honour redirects via `httpx`, and record `final_url` in their crawl summary logs.
  - Extracts `<a href="…">` values with `lxml`, normalises relative paths with `urljoin`, and bulk-inserts unique URLs into the `urls` table.
  - Marks domains with `domain_crawled` or `crawl_failed`; insert retries and PostgreSQL writes are resilient to transient errors.
  - `--log-urls` promotes discovered links to info-level log entries for easier auditing across distributed workers.

### extract_certstream.py
- **Purpose:** Listen to the public Certstream feed and add observed hostnames to PostgreSQL.
- **CLI:** `python tools/extract_certstream.py --postgres-dsn "postgresql+asyncpg://..."`
- **Details:**
  - Requires network access to `wss://certstream.calidog.io`.
  - On each certificate update event, strips leading `*.` from SAN entries and inserts every domain into the domains table (unique constraint on domain name).
  - The callback opens a short-lived PostgreSQL connection per message; keep an eye on connection limits.

### generate_sitemap.py
- **Purpose:** Build a sitemap XML by visiting a seed page with Selenium and optionally merging with an existing file.
- **CLI:** `python tools/generate_sitemap.py --url https://app.example.com --input sitemap.xml`
- **Details:**
  - Launches headless Chromium via Selenium, scrapes all anchor `href`s, and filters for the `purplepee.co`/`api.purplepee.co` hosts.
  - If `--input` exists, previously listed URLs are merged before writing the XML back to disk.
- **Prerequisites:** Chrome/Chromium binary, compatible `chromedriver`, and a writable output file path.

### generate_qrcode.py
- **Purpose:** Store a base64-encoded PNG QR code that points to each HTTPS domain.
- **CLI:** `python tools/generate_qrcode.py --worker 4 --postgres-dsn "postgresql+asyncpg://..."`
- **Details:**
  - Targets domains without a `qrcode` field; excludes punycode entries via regex.
  - Creates `https://<domain>` QR codes (`pyqrcode`) and saves them back to PostgreSQL with an `updated_at` timestamp.

### screenshot_scraper.py
- **Purpose:** Capture full-page screenshots of HTTPS sites and attach the file name to each domain record.
- **CLI:** `python tools/screenshot_scraper.py --postgres-dsn "postgresql+asyncpg://..."`
- **Details:**
  - Iterates over domains lacking `image`/`image_scan_failed`, renders them in headless Chromium, executes inline/external JavaScript, and saves PNGs into `screenshots/`.
  - Successful runs write the image filename and `updated_at` timestamp; failures get `image_scan_failed`.
- **Prerequisites:** Chromium + chromedriver, write access to `screenshots/`, and permissive network egress.

### extract_graph.py
- **Purpose:** Produce a graph of related domains (SSL SANs, CNAMEs, MX, NS) starting from a seed domain.
- **CLI:** `python tools/extract_graph.py --host mongodb.internal --domain example.com`
- **Details:** Uses MongoDB’s `$graphLookup` aggregation to discover relationships and prints a `nodes`/`edges` JSON structure suitable for visualisation.

## Enrichment Pipelines

### extract_records.py
- **Purpose:** Resolve standard DNS records (`A`, `AAAA`, `MX`, `NS`, `SOA`, `CNAME`) for domains missing an `updated_at` timestamp.
- **CLI:**
  - Queue jobs to RabbitMQ: `python tools/extract_records.py --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --queue-name dns_records --purge-queue`
  - Run distributed workers: `python tools/extract_records.py --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --service --worker 4 --concurrency 200 --log-records`
  - Direct mode: `python tools/extract_records.py --postgres-dsn "postgresql+asyncpg://..." --worker 4 --concurrency 200`
- **Details:**
  - Uses RabbitMQ to distribute domains across async workers; workers resolve DNS in parallel via `asyncio.to_thread` and write results through SQLModel/PostgreSQL.
  - Stores DNS records in separate tables (a_records, mx_records, etc.) with foreign keys to domains, clears stale claim markers, and logs per-domain summaries.
  - Emits STOP messages so service instances shut down cleanly once the queue is drained.

### banner_grabber.py
- **Purpose:** Fetch SSH banners from IPs referenced by domain A records stored in PostgreSQL.
- **CLI:** `python tools/banner_grabber.py --worker 8 --batch 100 --port 22`
- **Details:**
  - Reads `POSTGRES_DSN` from `.env` or environment variable, selects domains whose `port_services` include the target port (default: 22), and skips domains that already have a banner.
  - Uses an `asyncpg` connection pool and asyncio workers (controlled via `--worker`) for efficient concurrent processing.
  - Coordinates workers with the `banner_scan_state` helper table to ensure each domain is claimed exactly once during scanning.
  - Persists successful banners to the domain record, records failures with timestamps, and supports retries via `--retry-failed`.

### extract_header.py
- **Purpose:** Collect HTTP response headers with redirect awareness and persist the final URL and redirect chain.
- **CLI:**
  - Queue jobs to RabbitMQ: `python tools/extract_header.py --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --queue-name header_scans --purge-queue`
  - Run distributed workers: `python tools/extract_header.py --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --service --worker 4 --concurrency 200 --log-headers`
  - Direct mode: `python tools/extract_header.py --postgres-dsn "postgresql+asyncpg://..." --worker 4 --concurrency 200`
- **Details:**
  - Uses RabbitMQ to distribute header scan jobs; workers share an async HTTP client with redirect following and configurable timeouts.
  - Stores headers, HTTP status/version, final URL, and optional redirect chain, clearing stale `header_scan_failed` markers on success.
  - `--log-headers` dumps the captured header map for observability; STOP sentinel messages let service instances exit once queues are drained.

### extract_geoip.py
- **Purpose:** Enrich A-record domains with GeoIP data from a local MaxMind database using PostgreSQL.
- **CLI:**
  - Queue jobs to RabbitMQ: `python tools/extract_geoip.py --postgres-dsn "postgresql+asyncpg://..." --input /path/GeoLite2-City.mmdb --rabbitmq-url amqp://guest:guest@rabbitmq/ --queue-name geoip_enrichment --purge-queue`
  - Run distributed workers: `python tools/extract_geoip.py --postgres-dsn "postgresql+asyncpg://..." --input /path/GeoLite2-City.mmdb --rabbitmq-url amqp://guest:guest@rabbitmq/ --service --worker 4 --concurrency 25`
  - Direct mode: `python tools/extract_geoip.py --postgres-dsn "postgresql+asyncpg://..." --input /path/GeoLite2-City.mmdb --worker 4 --concurrency 25`
- **Details:**
  - Uses RabbitMQ (optional) to distribute GeoIP enrichment jobs so workers can scale horizontally; direct mode keeps the same claim loop without a broker.
  - Workers atomically claim domains via `geo_lookup_started`, write GeoPoint data to separate tables with foreign keys, clear failure markers on success, and record misses under `geo_lookup_failed`.
  - Supports configurable claim retry windows via `--claim-timeout` and honours the MaxMind City database path supplied via `--input`.
- **Prerequisites:** A GeoIP2/City `.mmdb` file (GeoLite2 or commercial) accessible on disk.

### extract_whois.py
- **Purpose:** Retrieve ASN/WHOIS data for domains or IP addresses using PostgreSQL.
- **CLI:** `python tools/extract_whois.py --target domains --worker 4 --postgres-dsn "postgresql+asyncpg://..."`
- **Details:**
  - Workers claim domains missing WHOIS data, query the `ipwhois` library, and store the response in the whois_records table with an `updated_at` timestamp.
  - For IP address lookups, WHOIS results are written to subnet lookup tables and enrichment flags are updated.

### ssl_cert_scanner.py
- **Purpose:** Perform TLS handshakes, capture certificate metadata, and test supported protocol versions.
- **CLI:**
  - Queue jobs to RabbitMQ: `python tools/ssl_cert_scanner.py --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --queue-name ssl_scans --purge-queue`
  - Run distributed workers: `python tools/ssl_cert_scanner.py --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --service --worker 4 --concurrency 100 --log-tls`
  - Direct mode: `python tools/ssl_cert_scanner.py --postgres-dsn "postgresql+asyncpg://..." --worker 4 --concurrency 100`
- **Details:**
  - Uses RabbitMQ to distribute scan jobs; workers reuse async TLS clients and restrict concurrency via semaphores.
  - Extracts issuer/subject/SAN data, OCSP/CRL endpoints, handshake metadata, and per-protocol acceptance (TLS 1.0–1.3) for each successful scan.
  - Stores results in ssl_data and ssl_subject_alt_names tables, clears failure markers, and logs summary lines (with optional verbose certificate dumps).

### masscan_scanner.py
- **Purpose:** Run the `masscan` binary against batches of claimed IPs and save any open ports.
- **CLI:** `python tools/masscan_scanner.py --workers 4 --input ports.txt --postgres-dsn "postgresql+asyncpg://..." --batch 500 --rate 2000`
- **Details:**
  - Requires the `masscan` executable and appropriate privileges (often `CAP_NET_RAW`).
  - Batches IPs lacking port data, runs `masscan -oJ`, parses the JSON, and updates the port_services table with foreign keys to domains.
  - Marks domains as `claimed` while scanning to avoid duplication.

## Security & Monitoring

### cve_2019_19781_scanner.py
- **Purpose:** Check Citrix ADC/NetScaler appliances for the CVE-2019-19781 directory traversal.
- **CLI:** `python tools/cve_2019_19781_scanner.py 192.0.2.0/24 443`
- **Details:**
  - Expands the target CIDR into IPv4s, spawns multiple processes, and requests `/vpn/../vpns/cfg/smb.conf`.
  - Prints coloured output when the response hints at vulnerability. (Note: the current implementation hardcodes port `443` when launching workers.)
- **Prerequisites:** Open network path to the targets; run from a network vantage point that mimics attacker traffic.

## Typical End-to-End Workflow

1. **Seed data:** Run `import_domains.py`, `import_ip.py`, and `insert_asn.py` as needed to populate PostgreSQL.
2. **Derive domains:** Execute `extract_domains.py` to turn stored URLs into registerable domains.
3. **Resolve DNS:** Use `extract_records.py` (and optionally `import_records.py`) to enrich each domain with DNS answers stored in relational tables.
4. **Network enrichment:** Launch `extract_geoip.py`, `extract_header.py`, `ssl_cert_scanner.py`, `extract_whois.py`, `banner_grabber.py`, and `masscan_scanner.py` to gather network metadata.
5. **Crawling & reporting:** Run `crawl_urls.py`, `generate_qrcode.py`, `screenshot_scraper.py`, and `generate_sitemap.py` for additional context and presentation outputs.
6. **Continuous discovery:** Keep `extract_certstream.py` (and optional security checks like `cve_2019_19781_scanner.py`) running to ingest newly observed assets.

All scripts are idempotent: they rely on marker fields (`*_failed`, `claimed`, `updated_at`, etc.) so that interrupting and re-running them is safe. The PostgreSQL schema uses foreign keys and proper constraints to maintain data integrity. Validate new configurations with a small worker count before scaling out.
