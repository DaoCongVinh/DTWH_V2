-- ============================================================================
-- Load DateDim data from CSV
-- This script loads the date_dim data into the DateDim table
-- ============================================================================

USE dbStaging;

-- For now, just verify the table exists
-- DateDim will be loaded by the loader application
SELECT COUNT(*) as table_status FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA='dbStaging' AND TABLE_NAME='DateDim';

