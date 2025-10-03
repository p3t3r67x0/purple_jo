# PostgreSQL Crawler Setup Instructions

## Important: Run from Project Root

All commands should be run from the **project root directory** (`/opt/git/purple_jo`), not from the `tools/` directory.

## Commands to Run

### 1. Navigate to project root
```bash
cd /opt/git/purple_jo
```

### 2. Set up crawl status table
```bash
python tools/setup_crawler_postgres.py \
  --postgres-dsn "postgresql+asyncpg://netscanner:pass@localhost/netscanner" \
  --populate
```

### 3. Check statistics
```bash
python tools/setup_crawler_postgres.py \
  --postgres-dsn "postgresql+asyncpg://netscanner:pass@localhost/netscanner" \
  --stats
```

### 4. Run the PostgreSQL crawler
```bash
python tools/crawl_urls_postgres.py \
  --postgres-dsn "postgresql+asyncpg://netscanner:pass@localhost/netscanner" \
  --worker 2 \
  --concurrency 100 \
  --limit 1000
```

### Example with different parameters
```bash
# Small test run
python tools/crawl_urls_postgres.py \
  --postgres-dsn "postgresql+asyncpg://netscanner:pass@localhost/netscanner" \
  --worker 1 \
  --concurrency 50 \
  --limit 100

# Production run
python tools/crawl_urls_postgres.py \
  --postgres-dsn "postgresql+asyncpg://netscanner:pass@localhost/netscanner" \
  --worker 4 \
  --concurrency 500 \
  --limit 10000
```

## Why Run from Project Root?

The PostgreSQL models and database configuration are in the main `app/` directory, which requires the project dependencies. Running from the project root ensures:

1. Access to the main `requirements.txt` with SQLModel and SQLAlchemy
2. Proper import paths for the app modules
3. Access to the PostgreSQL models and database configuration

## Troubleshooting

If you get import errors:
1. Make sure you're in `/opt/git/purple_jo` (not in `tools/`)
2. Make sure the main project dependencies are installed
3. Check that the PostgreSQL database is running and accessible