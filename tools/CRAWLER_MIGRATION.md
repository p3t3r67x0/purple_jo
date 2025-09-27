# Crawler Migration to PostgreSQL

This directory contains the migrated crawler that uses PostgreSQL instead of MongoDB.

## Files

- `crawl_urls_postgres.py` - New PostgreSQL-based crawler (replaces `crawl_urls.py`)
- `setup_crawler_postgres.py` - Migration helper script

## Migration Steps

### 1. Update Database Schema

The PostgreSQL models have been updated with new tables:
- `urls` - Stores discovered URLs (replaces MongoDB `url_data.url`)
- `crawl_status` - Tracks domain crawl status (replaces MongoDB `ip_data.dns` crawl fields)

### 2. Set Up Crawl Status

Before running the new crawler, populate the crawl status table:

```bash
python tools/setup_crawler_postgres.py --postgres-dsn "postgresql+asyncpg://user:pass@host/db" --populate
```

### 3. Run PostgreSQL Crawler

The new crawler supports both RabbitMQ and direct modes:

#### RabbitMQ Mode (Recommended for Production)

Enqueue domains:
```bash
python tools/crawl_urls_postgres.py \
  --postgres-dsn "postgresql+asyncpg://user:pass@host/db" \
  --rabbitmq-url "amqp://guest:guest@localhost:5672/" \
  --enqueue \
  --worker 4
```

Run workers:
```bash
python tools/crawl_urls_postgres.py \
  --postgres-dsn "postgresql+asyncpg://user:pass@host/db" \
  --rabbitmq-url "amqp://guest:guest@localhost:5672/" \
  --worker 4 \
  --concurrency 500
```

#### Direct Mode (for smaller batches)

```bash
python tools/crawl_urls_postgres.py \
  --postgres-dsn "postgresql+asyncpg://user:pass@host/db" \
  --worker 4 \
  --concurrency 500 \
  --limit 10000
```

### 4. Monitor Progress

Check crawl statistics:
```bash
python tools/setup_crawler_postgres.py --postgres-dsn "postgresql+asyncpg://user:pass@host/db" --stats
```

## Key Changes from MongoDB Version

1. **Database Models**: Uses SQLModel instead of MongoDB documents
2. **URL Storage**: URLs stored in dedicated `urls` table with proper indexing
3. **Crawl Tracking**: Domain crawl status tracked in `crawl_status` table
4. **Connection Handling**: Uses SQLAlchemy async engine with connection pooling
5. **Error Handling**: Improved error handling for PostgreSQL constraints
6. **Domain Claiming**: Prevents duplicate crawling by claiming domains atomically

## Performance Notes

- The PostgreSQL version maintains the same high-throughput architecture
- Connection pooling optimized for high concurrency
- Unique constraints prevent duplicate URL storage
- Proper indexing on domain names and crawl status for fast lookups
- **Domain Claiming**: Atomic domain claiming prevents multiple workers from crawling the same domain
  - Uses PostgreSQL's `ON CONFLICT DO NOTHING` for race-condition-free claiming
  - Only unclaimed domains (no `crawl_status` record) are selected for processing
  - Eliminates duplicate work across multiple worker processes

## Rollback

If you need to rollback to the MongoDB version:
1. Keep the original `crawl_urls.py` file
2. The MongoDB data remains unchanged
3. Switch back to using the original script

## Verification

After migration, you can verify the data:

```sql
-- Check domain count
SELECT COUNT(*) FROM domains;

-- Check crawl status distribution
SELECT 
  COUNT(*) as total,
  COUNT(domain_crawled) as crawled,
  COUNT(crawl_failed) as failed
FROM crawl_status;

-- Check URL count
SELECT COUNT(*) FROM urls;
```