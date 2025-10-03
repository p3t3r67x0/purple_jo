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
export POSTGRES_DSN="postgresql+asyncpg://username:password@localhost/netscanner"
```

**Quick Setup:**
1. Install dependencies: `pip install -r tools/requirements.txt`
2. Set up database: `alembic upgrade head`
3. Configure environment: Add `POSTGRES_DSN` to your `.env` file
4. Run tools with appropriate worker counts based on your system resources

**Common Environment Variables:**
- `POSTGRES_DSN` - PostgreSQL connection string (required)
- `RABBITMQ_URL` - RabbitMQ connection for distributed processing (optional)
- `LOG_LEVEL` - Logging verbosity (default: INFO)

**Service Units:** example systemd service and environment files live under `deploy/systemd/` for the RabbitMQ-backed workers (`crawl_urls_postgres`, `extract_geoip`, `extract_header`, `extract_whois`, `ssl_cert_scanner`). Copy the `.env.example` file, fill in credentials, and install the corresponding `.service` unit to run a microservice continuously. Each service also has a companion `*-publisher.service` for queue seeding; the shared env files provide `PUBLISHER_*` knobs for worker-count, batch size, and purge behaviour.

**Database Requirements:**
- PostgreSQL 12+ with async support via asyncpg driver
- Database schema initialized via Alembic migrations (`alembic upgrade head`)
- **Consolidated Migration System:** All database changes are now managed through a single comprehensive migration file (`202509271600_consolidated_schema.py`) for simplified setup and maintenance
- Proper indexes and constraints are automatically created by the migration system
- Connection pooling is configured per tool for optimal performance
- QR code support is automatically enabled via schema migration or self-healing table updates

Some scripts also depend on external binaries or services (for example `masscan`, `chromedriver`, or a MaxMind GeoIP database); those prerequisites are called out in their respective sections.

## Inventory At A Glance

| Script                      | Category      | Purpose                                                                               |
| --------------------------- | ------------- | ------------------------------------------------------------------------------------- |
| `banner_grabber.py`         | Enrichment    | Capture SSH banners for domains with open port 22.                                    |
| `crawl_urls_postgres.py`    | Crawling      | RabbitMQ microservice that crawls domains and stores discovered URLs in PostgreSQL.   |
| `cve_2019_19781_scanner.py` | Security      | Probe Citrix ADC appliances for the CVE-2019-19781 path traversal bug.                |
| `decode_idna.py`            | Normalisation | Convert punycode (`xn--`) hostnames in PostgreSQL back to Unicode.                    |
| `extract_certstream.py`     | Acquisition   | Subscribe to Certstream and ingest newly seen domains.                                |
| `extract_domains.py`        | Normalisation | Derive domain names from saved URLs.                                                  |
| `extract_geoip.py`          | Enrichment    | RabbitMQ microservice that enriches domains with MaxMind GeoIP data.                  |
| `extract_header.py`         | Enrichment    | RabbitMQ microservice that issues HTTP `HEAD` requests and stores response headers.   |
| `extract_records.py`        | Enrichment    | Resolve common DNS record types for domains lacking data.                             |
| `extract_whois.py`          | Enrichment    | RabbitMQ microservice that enriches domains with WHOIS/ASN information.               |
| `generate_qrcode.py`        | Reporting     | Generate base64 PNG QR codes for HTTPS URLs (migrated to PostgreSQL).                 |
| `generate_sitemap.py`       | Reporting     | Merge Selenium-discovered URLs with an existing sitemap.                              |
| `import_domains.py`         | Ingestion     | Seed the URL collection from a plaintext list.                                        |
| `import_ip.py`              | Ingestion     | Insert IPv4 addresses (or CIDR ranges) into PostgreSQL subnet tables.                 |
| `import_records.py`         | Ingestion     | Replay JSON lines with DNS answers into PostgreSQL DNS tables.                        |
| `insert_asn.py`             | Ingestion     | Load AS numbers into PostgreSQL ASN tables.                                           |
| `masscan_scanner.py`        | Security      | Run `masscan` against claimed IPs and persist open-port data.                         |
| `screenshot_scraper.py`     | Reporting     | Capture Chrome screenshots of HTTPS landing pages.                                    |
| `ssl_cert_scanner.py`       | Enrichment    | RabbitMQ microservice that captures TLS certificates, cipher suites, and TLS support. |

## Utility Scripts

| Script                         | Category | Purpose                                                              |
| ------------------------------ | -------- | -------------------------------------------------------------------- |
| `add_qrcode_column.py`         | Database | Add the qrcode column to the domains table (one-time setup utility). |
| `fix_alembic_consolidation.sh` | Database | Fix Alembic version references after migration consolidation.        |

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
- **Purpose:** Populate the `subnet_lookups` table with IPv4 ranges sourced from plain text lists.
- **CLI:** `python tools/import_ip.py --input ipv4.txt --postgres-dsn "postgresql+asyncpg://..."`
- **Details:**
  - Normalises each entry into a CIDR range, storing `cidr`, `ip_start`, and `ip_end` values alongside a `source` marker.
  - Accepts individual IPs (`198.51.100.10`) and CIDR blocks (`198.51.100.0/24`), coalescing them into single-row ranges.
- **Prerequisites:** Input file with one IP or CIDR per line.

### insert_asn.py
- **Purpose:** Seed the ASN-related tables with AS numbers for later enrichment.
- **CLI:** `python tools/insert_asn.py --input asn.txt --postgres-dsn "postgresql+asyncpg://..."`
- **Details:** Stores each ASN along with an insertion timestamp; duplicates are skipped.

### import_records.py
- **Purpose:** Replay historical DNS observations (JSON lines) into the PostgreSQL DNS tables.
- **CLI:** `python tools/import_records.py --input records.jsonl --postgres-dsn "postgresql+asyncpg://..."`
- **Details:**
  - Parses JSONL rows containing `query_name`, `resp_type`, and `data`, lower-casing values to match the relational schema.
  - Ensures the associated domain exists, then inserts unique DNS records into `a_records`, `aaaa_records`, `mx_records`, `ns_records`, `soa_records`, or `cname_records`.
  - Automatically updates the parent domain’s `updated_at` timestamp whenever new data is stored.
- **Prerequisites:** Structured JSONL export in the resolver output format used across the project.

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

### crawl_urls_postgres.py
- **Purpose:** Crawl pending domains, follow redirects, and persist discovered links in the `urls` table.
- **CLI:**
  - Publish jobs: `python tools/crawl_urls_postgres.py publish --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --worker-count 8 --purge-queue`
  - Run workers: `python tools/crawl_urls_postgres.py serve --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --worker 4 --concurrency 500 --request-timeout 10 --verbose-urls`
- **Details:**
  - Publisher drains uncrawled domains (no `crawl_status` entry) and emits STOP sentinels so service instances exit cleanly once the queue empties.
  - Workers share an async HTTP client, attempt HTTPS/HTTP in order, extract hyperlinks with BeautifulSoup, and upsert results into `urls` with retry-on-conflict logic.
  - `--max-redirects`, `--max-retries`, and timeout options allow fine-grained crawl control; `--verbose-urls` promotes every discovered link to INFO for auditing.

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
- **CLI:** `python tools/generate_qrcode.py --workers 4`
- **Details:**
  - **Migrated from MongoDB to PostgreSQL** for consistency with the rest of the project.
  - Automatically creates the `qrcode` column in the domains table if it doesn't exist.
  - Targets domains without a `qrcode` field; excludes punycode entries via regex matching.
  - Creates `https://<domain>` QR codes using `pyqrcode` and saves them back to PostgreSQL with an `updated_at` timestamp.
  - Uses async connection pooling and multiprocessing for efficient batch processing.
  - Reads database configuration from `POSTGRES_DSN` environment variable or `.env` file.

### screenshot_scraper.py
- **Purpose:** Capture full-page screenshots of HTTPS sites and attach the file name to each domain record.
- **CLI:** `python tools/screenshot_scraper.py --postgres-dsn "postgresql+asyncpg://..." [--limit 100]`
- **Details:**
  - Ensures the `image`/`image_scan_failed` columns exist, queries PostgreSQL for domains missing screenshots, and executes inline/external JavaScript before taking PNG captures into `screenshots/`.
  - Successful runs write the image filename and `updated_at` timestamp; failures store `image_scan_failed`.
- **Prerequisites:** Chromium + chromedriver, write access to `screenshots/`, and permissive network egress.

## Enrichment Pipelines

### extract_records.py
- **Purpose:** Resolve standard DNS records (`A`, `AAAA`, `MX`, `NS`, `SOA`, `CNAME`, `TXT`) for domains missing an `updated_at` timestamp.
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
  - Publish jobs: `python tools/extract_header.py publish --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --worker-count 8 --purge-queue`
  - Run workers: `python tools/extract_header.py serve --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --worker 4 --concurrency 200 --request-timeout 5`
- **Details:**
  - Publisher batches domains that still lack stored headers and enqueues STOP sentinels after the work payload.
  - Workers share an async httpx client with redirect following, persist headers/status/final URL, and clear `header_scan_failed` markers.
  - Optional `--log-headers` dumps the header map to the log; see `deploy/systemd/extract_header.service` for a production supervisor.
  - Hosts that refuse `HEAD` (or fail TLS with the default settings) are logged as warnings; re-run with `--verbose --log-headers` to surface the underlying `httpx` exception before deciding whether to tweak timeouts or disable verification.

### extract_geoip.py
- **Purpose:** Enrich A-record domains with GeoIP data sourced from a local MaxMind database.
- **CLI:**
  - Publish jobs: `python tools/extract_geoip.py publish --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --worker-count 8 --purge-queue`
  - Run workers: `python tools/extract_geoip.py serve --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --mmdb-path /var/lib/GeoLite2-City.mmdb --worker 4 --concurrency 25`
- **Details:**
  - Publisher batches pending domains directly from PostgreSQL, emits JSON jobs with integer domain IDs plus all discovered A-record IPs, and posts stop sentinels for each worker.
  - Workers stream jobs from RabbitMQ, resolve GeoIP metadata via `geoip2` in a thread pool, and persist both denormalised fields on `domains` and detailed rows in `geo_points`.
  - When no match is found, the worker marks the domain as touched so it is not re-queued immediately; verbose logging highlights lookup misses.
- **Prerequisites:** A GeoIP2/City `.mmdb` file (GeoLite2 or commercial) readable by the worker processes; see `deploy/systemd/extract_geoip.service` for a systemd example.

### extract_whois.py
- **Purpose:** Enrich domains with ASN/WHOIS metadata sourced from the `ipwhois` library.
- **CLI:**
  - Publish jobs: `python tools/extract_whois.py publish --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --worker-count 8 --purge-queue`
  - Run workers: `python tools/extract_whois.py serve --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --worker 4 --concurrency 10`
- **Details:**
  - Publisher batches domains that lack entries in `whois_records` and enqueues JSON jobs with the domain ID and all associated A-record IPs.
  - Workers consume from RabbitMQ, resolve WHOIS data via `ipwhois` (first successful IP wins), and upsert results into `whois_records` so hosts without data are not re-queued repeatedly.
  - Warnings indicate that none of the IPs returned WHOIS metadata; rerun with `--verbose` to surface the underlying exception before deciding on retries.

### ssl_cert_scanner.py
- **Purpose:** Perform TLS handshakes, capture certificate metadata, and record supported cipher/protocol combinations.
- **CLI:**
  - Publish jobs: `python tools/ssl_cert_scanner.py publish --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --worker-count 8 --purge-queue`
  - Run workers: `python tools/ssl_cert_scanner.py serve --postgres-dsn "postgresql+asyncpg://..." --rabbitmq-url amqp://guest:guest@rabbitmq/ --worker 4 --concurrency 100 --prefetch 400`
- **Details:**
  - Publisher streams domains needing TLS checks, pushes them to RabbitMQ, and appends stop sentinels so worker pools exit cleanly when the queue drains.
  - Workers reuse async HTTP/TLS clients, measure handshake success per TLS version, and persist issuer/subject/SAN metadata to `ssl_data` and `ssl_subject_alt_names` tables.
  - Optional `--log-tls` flag emits the full certificate payload to the logs; see `deploy/systemd/ssl_cert_scanner.service` for a production unit file.

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

1. **Database setup:** Initialize schema with `alembic upgrade head`
2. **Seed data:** Run `import_domains.py`, `import_ip.py`, and `insert_asn.py` as needed to populate PostgreSQL.
3. **Derive domains:** Execute `extract_domains.py` to turn stored URLs into registerable domains.
4. **Resolve DNS:** Use `extract_records.py` to enrich each domain with DNS answers stored in relational tables.
5. **Network enrichment:** Launch `extract_geoip.py`, `extract_header.py`, `ssl_cert_scanner.py`, `extract_whois.py`, `banner_grabber.py`, and `masscan_scanner.py` to gather network metadata.
6. **Crawling & reporting:** Run `crawl_urls_postgres.py`, `generate_qrcode.py`, `screenshot_scraper.py`, and `generate_sitemap.py` for additional context and presentation outputs.
7. **Continuous discovery:** Keep `extract_certstream.py` (and optional security checks like `cve_2019_19781_scanner.py`) running to ingest newly observed assets.

**Performance Tips:**
- Start with small worker counts (2-4) and scale up based on system resources
- Monitor PostgreSQL connection limits when running multiple tools simultaneously  
- Use RabbitMQ for distributed processing when scaling across multiple machines
- Run database-intensive operations during off-peak hours for better performance

## Database Utilities

### add_qrcode_column.py
- **Purpose:** One-time utility to add the `qrcode` column to the domains table if not present.
- **CLI:** `python tools/add_qrcode_column.py`
- **Details:**
  - Automatically detects if the `qrcode` column exists in the domains table.
  - Adds the column as `TEXT NULL` if missing, enabling QR code functionality.
  - Safe to run multiple times; only executes ALTER TABLE if needed.
  - Uses the same PostgreSQL connection configuration as other tools.

### Migration System
- **Consolidated Migrations:** The project now uses a single comprehensive Alembic migration (`202509271600_consolidated_schema.py`) instead of multiple incremental migrations.
- **Setup:** Run `alembic upgrade head` to initialize or update the database schema.
- **Troubleshooting:** If you encounter Alembic version errors after consolidation, run the `fix_alembic_consolidation.sh` script to update version references.

## Notes

All scripts are idempotent: they rely on marker fields (`*_failed`, `claimed`, `updated_at`, etc.) so that interrupting and re-running them is safe. The PostgreSQL schema uses foreign keys and proper constraints to maintain data integrity. Validate new configurations with a small worker count before scaling out.

**Recent Updates:**
- **PostgreSQL Migration Complete:** All tools now use PostgreSQL exclusively, with MongoDB dependencies removed.
- **QR Code Support:** The `generate_qrcode.py` tool has been fully migrated to PostgreSQL with automatic schema updates.
- **Simplified Migrations:** Database schema management is now handled through a single consolidated Alembic migration for easier setup and maintenance.
