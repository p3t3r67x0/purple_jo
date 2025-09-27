-- Database initialization script for DNS extraction services
-- This script ensures the database is ready for the DNS services

-- Create database if not exists (already handled by POSTGRES_DB env var)
-- But we can set some optimizations

-- Optimize PostgreSQL for DNS workload
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.7;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET effective_io_concurrency = 200;

-- Apply settings
SELECT pg_reload_conf();

-- Create a function to show table sizes (useful for monitoring)
CREATE OR REPLACE FUNCTION show_table_sizes()
RETURNS TABLE(
    table_name text,
    row_count bigint,
    total_size text,
    index_size text,
    toast_size text
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        schemaname||'.'||tablename as table_name,
        n_tup_ins - n_tup_del as row_count,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
        pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as index_size,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - 
                      pg_relation_size(schemaname||'.'||tablename) - 
                      pg_indexes_size(schemaname||'.'||tablename)) as toast_size
    FROM pg_stat_user_tables
    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
END;
$$ LANGUAGE plpgsql;

-- Create a function to show DNS processing stats
CREATE OR REPLACE FUNCTION dns_processing_stats()
RETURNS TABLE(
    total_domains bigint,
    domains_with_records bigint,
    domains_without_records bigint,
    processing_rate numeric
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_domains,
        COUNT(DISTINCT ar.domain_id) as domains_with_records,
        COUNT(*) - COUNT(DISTINCT ar.domain_id) as domains_without_records,
        ROUND(COUNT(DISTINCT ar.domain_id)::numeric / COUNT(*)::numeric * 100, 2) as processing_rate
    FROM domains d
    LEFT JOIN a_records ar ON d.id = ar.domain_id;
END;
$$ LANGUAGE plpgsql;

-- Print welcome message
DO $$
BEGIN
    RAISE NOTICE 'DNS Record Extraction Database initialized successfully!';
    RAISE NOTICE 'Use SELECT * FROM show_table_sizes(); to monitor table growth';
    RAISE NOTICE 'Use SELECT * FROM dns_processing_stats(); to check processing progress';
END $$;