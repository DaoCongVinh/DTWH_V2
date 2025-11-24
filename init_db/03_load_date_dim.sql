-- ============================================================================
-- Load DateDim data from CSV
-- This script loads the date_dim data into the DateDim table
-- ============================================================================

USE dbStaging;

-- Check if DateDim table exists and is empty before loading
SELECT 'Checking DateDim table...' AS status;
SELECT COUNT(*) as current_rows FROM DateDim;

-- Load data from CSV file (this assumes MySQL LOAD DATA is available)
-- Note: Path might need adjustment depending on container/server setup
LOAD DATA LOCAL INFILE './services/loaderStaging/date_dim.csv'
INTO TABLE DateDim
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
(date_sk, full_date, day_since_2005, month_since_2005, day_of_week, 
 calendar_month, calendar_year, calendar_year_month, day_of_month, 
 day_of_year, week_of_year_sunday, year_week_sunday, week_sunday_start,
 week_of_year_monday, year_week_monday, week_monday_start, 
 quarter, month, holiday, day_type);

-- Verify the load
SELECT COUNT(*) as loaded_rows FROM DateDim;
SELECT * FROM DateDim LIMIT 5;

-- Show some sample data for verification
SELECT 
    date_sk,
    full_date,
    day_of_week,
    calendar_month,
    calendar_year,
    quarter,
    day_type
FROM DateDim 
WHERE full_date BETWEEN '2025-11-01' AND '2025-11-30'
ORDER BY full_date;

SELECT 'DateDim load completed successfully!' AS status;

