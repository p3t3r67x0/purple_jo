# Crawler & DNS Record Utilities

This guide explains how to run the two MongoDB-backed helpers in the `tools/` folder:

- `crawl_urls.py` &mdash; asynchronously crawl domains stored in MongoDB and capture the outbound links they expose.
- `utils/extract_records.py` &mdash; resolve common DNS records for domains and persist the results.

Both scripts expect the Mongo instance to expose two databases:

- `ip_data.dns` &mdash; contains documents with at least a `domain` field. Each tool looks for domains where their respective status fields are missing.
- `url_data.url` &mdash; receives the HTTP links collected by `crawl_urls.py`.

Before running either tool, install the dependencies listed in `tools/requirements.txt` (or the project-wide `requirements.txt`) inside your virtual environment:

```bash
pip install -r requirements.txt
```

## crawl_urls.py

### Purpose
Retrieve domains from `ip_data.dns`, fetch each domain's landing page over HTTP, extract absolute links, and insert them into `url_data.url`. The script marks domains as `domain_crawled` (or `crawl_failed`) to avoid duplicate work.

### Command-line options

| Option               | Required | Description                                                                |
| -------------------- | -------- | -------------------------------------------------------------------------- |
| `-w / --worker`      | yes      | Number of worker **processes** (multiprocessing pool) to launch.           |
| `-h / --host`        | yes      | MongoDB host name or IP (the script always connects on port 27017).        |
| `-c / --concurrency` | no       | Async tasks per worker process. Defaults to `10`; minimum enforced to `1`. |

### How it works
1. Counts the number of `ip_data.dns` documents where `domain_crawled` is missing.
2. Splits the pending domains into `worker` chunks and spawns a multiprocessing pool.
3. Each process runs an asyncio worker that:
   - Pulls domains from Mongo in batches (`$natural` order) using Motor.
   - Fetches `http://<domain>` using `httpx` with redirects enabled.
   - Extracts links via `lxml`, normalises them, and keeps only links with a host component.
   - Inserts the discovered links into `url_data.url` with a timestamp (duplicates are ignored).
   - Updates the source domain document with either `domain_crawled` or `crawl_failed`.

### Example usage

```bash
python tools/crawl_urls.py --worker 4 --host mongodb.internal --concurrency 25
```

Helpful tips:
- Provide a realistic number of workers that matches the MongoDB capacity; each worker opens its own client.
- `crawl_urls.py` only attempts plain HTTP (`http://`). If your domains require HTTPS-only resources, extend the script accordingly.
- Review the console output for `[WARN]` messages that indicate fetch errors or parsing issues.

## utils/extract_records.py

### Purpose
Look up common DNS records for domains in `ip_data.dns` that do not yet have an `updated` timestamp. The script records the results under fields such as `a_record`, `mx_record`, etc.

### Command-line options

| Option     | Required | Description                                                                  |
| ---------- | -------- | ---------------------------------------------------------------------------- |
| `--worker` | yes      | Number of **processes** to spawn. Determines how the domain list is divided. |
| `--host`   | yes      | MongoDB host name or IP (port 27017 is implied).                             |

### How it works
1. Estimates the total number of documents in `ip_data.dns` and divides the space evenly across the requested workers.
2. Each process:
   - Connects to MongoDB and retrieves a slice of domains without the `updated` field.
   - Performs DNS queries for the record types `A`, `AAAA`, `NS`, `MX`, `SOA`, and `CNAME` (timeouts are set to 1 second).
   - Uses `update_one(..., $addToSet=...)` so repeated runs do not duplicate records.
   - Inserts a new document if the domain does not yet exist in `ip_data.dns`.
   - Marks unsuccessful lookups with an appropriate timestamp via `update_failed`.
3. The parent process waits for all worker processes and reports their exit codes.

### Example usage

```bash
python tools/utils/extract_records.py --worker 8 --host mongodb.internal
```

Helpful tips:
- Because DNS queries are synchronous per domain, higher `--worker` values improve throughput at the cost of more simultaneous MongoDB connections.
- Timeout- or NXDOMAIN-related exceptions are silently skipped; check the console for `INFO` messages about domains with no records.
- Make sure outbound DNS queries are permitted from the machine running the script.

## Typical workflow
1. Populate `ip_data.dns` with the target domains (e.g. via `tools/import_domains.py`).
2. Run `tools/utils/extract_records.py` to enrich each domain with DNS information.
3. Execute `tools/crawl_urls.py` to crawl unresolved domains and capture their outbound links.
4. Inspect MongoDB collections for the newly added data or rerun the scripts to pick up domains that were skipped earlier.

Both scripts are idempotent: they track processed domains and can be re-run safely. Use small worker counts first to validate connectivity before scaling up.
