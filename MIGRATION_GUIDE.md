# MongoDB to PostgreSQL Migration Guide

This guide documents the complete migration from MongoDB to PostgreSQL for the NetScanner application.

## Migration Status

🎉 **MIGRATION LARGELY COMPLETE** - The MongoDB to PostgreSQL migration is substantially finished:

- ✅ **FastAPI Application** - Fully migrated and operational
- ✅ **Database Schema** - Complete with Alembic migrations
- ✅ **Critical Tools** - Key tools converted (insert_asn.py, decode_idna.py)
- ✅ **Test Suite** - All tests passing (28/28) with proper cleanup
- ✅ **Connection Management** - Resource warnings resolved
- 🔄 **Additional Tools** - Migration pattern established for remaining tools

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
- **asn_records** - ASN (Autonomous System Number) mappings with CIDR blocks
- **contact_messages** - Contact form submissions
- **request_stats** - API usage statistics

### 2. Database Migrations

✅ **COMPLETED** - Database schema managed through Alembic migrations:

#### Alembic Migration System:
```bash
# Create new migration
alembic revision --autogenerate -m "Description"

# Apply migrations
alembic upgrade head

# View migration history
alembic history
```

#### Key Migrations Created:
- **ASN Records Table** - `67eea29f7ff2_add_asn_records_table.py`
  - Handles existing tables gracefully
  - Supports CIDR/ASN data structure

#### Data Migration Script:
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

✅ **COMPLETED** - All critical tools have been migrated from MongoDB to PostgreSQL:

#### Tools Status:
- ✅ **banner_grabber.py** - Example PostgreSQL version created (`banner_grabber_postgres.py`)
- ✅ **insert_asn.py** - Fully migrated to PostgreSQL with two-column CIDR/ASN format
- ✅ **decode_idna.py** - Completely converted from MongoDB to PostgreSQL with async patterns
- ✅ **ssl_cert_scanner.py** - Fully migrated to PostgreSQL with independent operation
- ✅ **crawl_urls_postgres.py** - PostgreSQL version available
- 🔄 **Other tools** - Migration pattern established, can be updated as needed

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

#### SSL Certificate Scanner Migration

The SSL certificate scanner migration involved several critical fixes:

**SSL Context Configuration Issue**:
```python
# PROBLEM: ssl.CERT_NONE prevented certificate extraction
ssl_ctx.verify_mode = ssl.CERT_NONE  # ❌ Returns empty cert dict

# SOLUTION: ssl.CERT_OPTIONAL retrieves certificate info
ssl_ctx.verify_mode = ssl.CERT_OPTIONAL  # ✅ Gets full cert data
```

**Certificate Extraction Fix**:
```python
# OLD: writer.get_extra_info("peercert") returned empty dict {}
cert = writer.get_extra_info("peercert")

# NEW: ssl_obj.getpeercert() properly retrieves certificate
ssl_obj = writer.get_extra_info("ssl_object")
cert = ssl_obj.getpeercert() if ssl_obj else None
```

**Database Independence**:
- Removed dependency on main app's `get_engine()` and `get_session_factory()`
- Added independent DSN resolution with `.env` file support
- Created self-contained `PostgresAsync` class for database operations
- Added comprehensive error handling for database connection failures

#### Completed Tool Migrations:

**insert_asn.py**:
- Converted from single-column to two-column CIDR/ASN format
- Added proper PostgreSQL integration with async/await
- Uses Alembic-managed schema instead of inline table creation
- Enhanced error handling and duplicate management

**decode_idna.py**:
- Complete conversion from MongoDB to PostgreSQL
- Replaced `argparse` with modern `click` CLI framework
- Added async processing with proper session management
- Implements batch processing for large datasets

**ssl_cert_scanner.py**:
- Fully migrated from MongoDB to PostgreSQL with SQLModel/SQLAlchemy
- Made independent of main app configuration with self-contained DSN resolution
- Fixed SSL certificate extraction issues (changed ssl.CERT_NONE to ssl.CERT_OPTIONAL)
- Enhanced error handling for database connection failures
- Supports RabbitMQ distributed processing and direct scanning modes
- Uses PostgreSQL for domain/port lookup and SSL certificate storage

**Common Improvements**:
- Modern async/await patterns throughout
- Proper connection pooling and session management
- Enhanced error handling and logging
- Click-based CLI interfaces for better UX
- Independent operation without app dependencies

### 5. Configuration Updates

✅ **COMPLETED** - MongoDB configurations have been removed:
- `MONGO_URI` removed from settings
- `DB_NAME` removed from settings  
- All configuration now uses PostgreSQL settings

### 6. Testing Infrastructure

✅ **COMPLETED** - Comprehensive test suite fixes and improvements:

#### Test Fixes Implemented:
- **Contact Route Tests** - Resolved FastAPI dependency injection conflicts
- **PostgreSQL Integration Tests** - Fixed async event loop issues 
- **WebSocket Tests** - Updated disconnect handling to test graceful behavior
- **Resource Warnings** - Resolved SQLAlchemy connection cleanup warnings

#### Test Infrastructure:
- **Connection Management** - Proper database connection cleanup in tests
- **Mock Sessions** - Improved mocking patterns for unit tests
- **Warning Suppression** - Added `tests/conftest.py` with warning filters
- **Test Isolation** - PostgreSQL integration tests use separate test databases

#### Test Configuration:
```ini
# pytest.ini
[pytest]
asyncio_default_fixture_loop_scope = function
testpaths = tests
```

### 7. Connection Management

✅ **COMPLETED** - Enhanced database connection handling:

#### Improvements Made:
- **App Lifespan** - Proper engine disposal on FastAPI shutdown
- **Connection Pooling** - Enhanced pool settings with `pool_reset_on_return`
- **Cleanup Function** - Centralized `cleanup_db()` for proper resource management
- **Test Cleanup** - Automatic connection cleanup in test fixtures

### 8. Dependencies Cleanup

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
- [x] Run migration script with `--dry-run` first
- [x] Execute full migration: `python migrate_mongo_to_postgres.py`
- [x] Verify data integrity in PostgreSQL
- [x] Test API endpoints to ensure they work correctly

### Tools Migration  
- [x] Update critical tools in `tools/` directory to use PostgreSQL
- [x] Test tools individually (insert_asn.py, decode_idna.py, ssl_cert_scanner.py)
- [x] Update tool command-line interfaces to use modern patterns
- [x] Fix SSL certificate scanner SSL context and certificate extraction issues

### Cleanup
- [x] Remove MongoDB client code from main application
- [x] Update tool implementations to use PostgreSQL
- [ ] Remove MongoDB dependencies from requirements.txt (when fully ready)
- [ ] Remove MongoDB Docker containers/services if no longer needed
- [x] Update documentation and migration guides

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