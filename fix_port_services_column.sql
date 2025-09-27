-- Manual SQL to fix the port_services table column mismatch
-- Run this in your PostgreSQL database

-- Step 1: Rename the 'protocol' column to 'service'
ALTER TABLE port_services RENAME COLUMN protocol TO service;

-- Step 2: Increase the column length from VARCHAR(16) to VARCHAR(255) to match the model
ALTER TABLE port_services ALTER COLUMN service TYPE VARCHAR(255);

-- Step 3: Update the Alembic version table to mark migrations as applied
-- (Run these if you used alembic stamp commands)
-- INSERT INTO alembic_version VALUES ('202501031000');
-- UPDATE alembic_version SET version_num = '0b7cd2b4eaff';