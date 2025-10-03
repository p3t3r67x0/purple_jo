# Domain Extraction Migration

The `extract_domains.py` script has been migrated from MongoDB to PostgreSQL.

## Key Changes

### Database Backend
- **Before**: Used MongoDB (`pymongo`, `url_data.url`, `ip_data.dns` collections)
- **After**: Uses PostgreSQL with SQLModel (`urls` and `domains` tables)

### New PostgreSQL Schema
- Added `domain_extracted` field to `urls` table to track processing status
- Uses existing `domains` table for storing extracted domains
- Proper indexing on `domain_extracted` for efficient queries

### Usage

#### Old MongoDB Command
```bash
python tools/extract_domains.py --workers 4 --host localhost
```

#### New PostgreSQL Command

First, run the schema migration to add the required column:
```bash
cd /opt/git/purple_jo
python tools/migrate_urls_schema.py \
  --postgres-dsn "postgresql+asyncpg://netscanner:pass@localhost/netscanner"
```

Then run the domain extraction:
```bash
python tools/extract_domains.py \
  --workers 4 \
  --postgres-dsn "postgresql+asyncpg://netscanner:pass@localhost/netscanner"
```

## Functionality

1. **Find Unprocessed URLs**: Selects URLs where `domain_extracted IS NULL`
2. **Extract Domains**: Uses regex to find domains in URL strings
3. **Filter IP Addresses**: Skips URLs that contain IP addresses instead of domains
4. **Store Domains**: Adds new domains to the `domains` table (skips duplicates)
5. **Mark Processed**: Sets `domain_extracted` timestamp on processed URLs

## Performance

- Maintains multiprocessing architecture for parallel domain extraction
- Uses PostgreSQL connection pooling for efficient database access
- Handles duplicate domains gracefully with proper error handling
- Processes URLs in batches to optimize memory usage

## Dependencies

- Requires main project dependencies (SQLModel, asyncpg)
- Must be run from project root directory for imports to work
- Uses async PostgreSQL operations wrapped in multiprocessing workers