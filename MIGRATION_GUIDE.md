# MongoDB to PostgreSQL Migration Guide

This guide documents the complete migration from MongoDB to PostgreSQL for the NetScanner application.

## Overview

The NetScanner application has been migrated from MongoDB to PostgreSQL to provide:
- Better data consistency and ACID transactions
- Improved query performance with proper indexing
- SQL-based analytics and reporting capabilities
- Better integration with modern Python async/await patterns

## Migration Components

### 1. Database Schema

The PostgreSQL schema is defined in `app/models/postgres.py` with the following main tables:

- **domains** - Main domain information
- **a_records** - IPv4 DNS records
- **aaaa_records** - IPv6 DNS records
- **ns_records** - Name server records
- **soa_records** - Start of Authority records  
- **mx_records** - Mail exchange records
- **cname_records** - Canonical name records
- **port_services** - Port and service information
- **ssl_data** - SSL certificate data
- **ssl_subject_alt_names** - Certificate alternative names
- **whois_records** - WHOIS information
- **geo_points** - Geographic location data

### 2. Data Migration Script

The `migrate_mongo_to_postgres.py` script handles the complete data migration:

```bash
# Run the migration
python migrate_mongo_to_postgres.py \
    --mongo-uri mongodb://localhost:27017 \
    --batch-size 1000

# Dry run to test migration
python migrate_mongo_to_postgres.py \
    --mongo-uri mongodb://localhost:27017 \
    --batch-size 1000 \
    --dry-run
```

#### Migration Features:
- **Batch processing** - Processes data in configurable batches
- **Error handling** - Continues on errors and reports statistics
- **Data transformation** - Converts MongoDB documents to PostgreSQL records
- **Relationship mapping** - Properly maps nested MongoDB data to relational tables
- **Duplicate handling** - Handles existing data gracefully

### 3. API Migration Status

✅ **COMPLETED** - The main FastAPI application has been fully migrated:
- All routes now use PostgreSQL via SQLModel
- Dependency injection provides PostgreSQL sessions
- Query layer uses SQLAlchemy for complex queries
- Full relationship loading with proper joins

### 4. Tools Migration

The tools in the `tools/` directory need to be migrated from MongoDB to PostgreSQL:

#### Tools Status:
- 🔄 **banner_grabber.py** - Example PostgreSQL version created (`banner_grabber_postgres.py`)
- ❌ **crawl_urls.py** - Needs migration
- ❌ **extract_domains.py** - Needs migration  
- ❌ **generate_qrcode.py** - Needs migration
- ❌ **screenshot_scraper.py** - Needs migration
- ❌ **ssl_cert_scanner.py** - Needs migration
- ❌ **extract_geoip.py** - Needs migration
- ❌ **masscan_scanner.py** - Needs migration
- ❌ **extract_whois.py** - Needs migration

#### Migration Pattern for Tools:

Replace MongoDB connections:
```python
# OLD MongoDB pattern
from pymongo import MongoClient
client = MongoClient(f'mongodb://{host}:27017')
db = client.ip_data

# NEW PostgreSQL pattern
import asyncpg
from app.db_postgres import get_session_factory
from app.models.postgres import Domain

session_factory = get_session_factory()
async with session_factory() as session:
    # Use SQLModel queries
    stmt = select(Domain).where(Domain.name == domain_name)
    result = await session.exec(stmt)
    domain = result.scalars().first()
```

### 5. Configuration Updates

✅ **COMPLETED** - MongoDB configurations have been removed:
- `MONGO_URI` removed from settings
- `DB_NAME` removed from settings  
- All configuration now uses PostgreSQL settings

### 6. Dependencies Cleanup

After migration is complete, remove MongoDB dependencies:

```bash
# Remove from requirements.txt
pip uninstall pymongo motor

# Update requirements.txt to remove:
# - pymongo==4.15.0
# - motor==3.x.x
```

## Migration Checklist

### Pre-Migration
- [ ] Backup existing MongoDB data
- [ ] Ensure PostgreSQL is set up and accessible
- [ ] Run Alembic migrations: `alembic upgrade head`
- [ ] Verify `POSTGRES_DSN` is configured correctly

### Data Migration
- [ ] Run migration script with `--dry-run` first
- [ ] Execute full migration: `python migrate_mongo_to_postgres.py`
- [ ] Verify data integrity in PostgreSQL
- [ ] Test API endpoints to ensure they work correctly

### Tools Migration  
- [ ] Update each tool in `tools/` directory to use PostgreSQL
- [ ] Test each tool individually
- [ ] Update any scripts or automation that calls these tools

### Cleanup
- [ ] Remove MongoDB client code from remaining files
- [ ] Remove MongoDB dependencies from requirements.txt
- [ ] Remove MongoDB Docker containers/services if no longer needed
- [ ] Update documentation and deployment scripts

## Post-Migration Verification

### 1. API Testing
```bash
# Test main endpoints
curl http://localhost:8000/query/example.com
curl http://localhost:8000/dns?page=1&page_size=10
curl http://localhost:8000/trends/requests
```

### 2. Database Verification
```sql
-- Check record counts
SELECT 'domains' as table_name, COUNT(*) as count FROM domains
UNION ALL
SELECT 'a_records', COUNT(*) FROM a_records
UNION ALL  
SELECT 'ssl_data', COUNT(*) FROM ssl_data;

-- Verify relationships
SELECT d.name, COUNT(a.id) as a_records_count
FROM domains d
LEFT JOIN a_records a ON d.id = a.domain_id
GROUP BY d.id, d.name
LIMIT 10;
```

### 3. Performance Testing
- Compare query performance between old MongoDB and new PostgreSQL
- Verify pagination works correctly
- Test search functionality

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify `POSTGRES_DSN` format: `postgresql+asyncpg://user:pass@host/db`
   - Check PostgreSQL service is running
   - Verify firewall/network access

2. **Migration Failures**
   - Check MongoDB connection and data format
   - Verify PostgreSQL schema is up to date
   - Check for data type mismatches

3. **Performance Issues**
   - Ensure proper indexes are created (handled by Alembic)
   - Consider adjusting PostgreSQL configuration for large datasets
   - Monitor connection pool settings

### Rollback Plan

If issues arise, you can temporarily rollback by:
1. Preserving MongoDB data during migration
2. Reverting application configuration to use MongoDB
3. Rolling back code changes

## Benefits After Migration

1. **Better Performance** - SQL queries with proper indexing
2. **Data Integrity** - ACID transactions and foreign key constraints  
3. **Analytics** - Complex SQL queries for reporting
4. **Ecosystem** - Better tooling and monitoring options
5. **Scaling** - PostgreSQL's proven scaling capabilities

## Next Steps

1. Monitor application performance post-migration
2. Optimize queries based on usage patterns  
3. Set up PostgreSQL monitoring and alerting
4. Consider read replicas for scaling if needed
5. Update backup strategies for PostgreSQL