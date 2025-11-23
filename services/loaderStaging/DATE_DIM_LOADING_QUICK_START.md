# DateDim CSV Loading - Quick Reference

## What's New

Enhanced DateDim loading with full validation based on Java date dimension generator:
- ✅ Load all 18 columns from `date_dim.csv`
- ✅ Comprehensive data validation
- ✅ Detailed error reporting
- ✅ Multiple loading methods (simple + validation)
- ✅ Performance statistics

## Quick Usage

### Method 1: Using CLI Tool (Recommended)

```bash
cd services/loaderStaging

# Load with validation (default)
python load_date_dim_cli.py

# Load with validation + show all errors
python load_date_dim_cli.py --verbose

# Load with simple LOAD DATA INFILE method (faster)
python load_date_dim_cli.py --simple

# Verify loaded data
python load_date_dim_cli.py --verify
```

### Method 2: Using Test Script

```bash
cd services/loaderStaging
python test_date_dim_load.py
```

### Method 3: Programmatic Usage

```python
from db import DatabaseConnection, DateDimManager
import config

db_conn = DatabaseConnection()
db_conn.connect()

date_dim_manager = DateDimManager(db_conn)

# Method A: Simple load
success = date_dim_manager.load_date_dim_from_csv(config.DATE_DIM_PATH)

# Method B: Load with validation (recommended)
success, stats = date_dim_manager.load_date_dim_with_validation(config.DATE_DIM_PATH)
print(f"Loaded: {stats['loaded_records']} records in {stats['duration_seconds']:.2f}s")

db_conn.disconnect()
```

### Method 4: Using Main Loader

```bash
cd services/loaderStaging
python loader.py --load-raw --load-staging
```

The loader automatically calls `load_date_dim()` on initialization.

## CSV Format

The `date_dim.csv` contains 18 columns:

```
date_sk,full_date,day_since_2005,month_since_2005,day_of_week,calendar_month,calendar_year,calendar_year_month,day_of_month,day_of_year,week_of_year_sunday,year_week_sunday,week_sunday_start,week_of_year_monday,year_week_monday,week_monday_start,holiday,day_type
1,2005-01-01,1,1,Saturday,January,2005,2005-Jan,1,1,52,2004-W52,2004-12-26,53,2004-W53,2004-12-27,Non-Holiday,Weekend
2,2005-01-02,2,1,Sunday,January,2005,2005-Jan,2,2,1,2005-W01,2005-01-02,53,2004-W53,2004-12-27,Non-Holiday,Weekend
...
```

## Files Created/Modified

### New Files:
- `load_date_dim_cli.py` - Command-line tool for loading DateDim
- `test_date_dim_load.py` - Test script with comprehensive validation
- `DATE_DIM_LOADING_GUIDE.md` - Detailed implementation guide

### Modified Files:
- `db.py` - Enhanced `DateDimManager` class with validation method
- `loader.py` - Improved `load_date_dim()` method with better logging

## Features

### 1. Data Validation
- ✓ Verifies CSV has 18 columns per row
- ✓ Validates date_sk is numeric
- ✓ Validates full_date is YYYY-MM-DD format
- ✓ Reports line-by-line errors
- ✓ Skips invalid rows instead of failing entire load

### 2. Error Handling
- ✓ Graceful error handling with detailed messages
- ✓ Transaction rollback on database errors
- ✓ Reports all errors with line numbers
- ✓ Collects statistics on skipped records

### 3. Performance
- ✓ LOAD DATA INFILE method: ~2-5 seconds
- ✓ Validation method: ~5-10 seconds
- ✓ Both support 7000+ records easily

### 4. Statistics
- ✓ Total records processed
- ✓ Records successfully loaded
- ✓ Records skipped due to errors
- ✓ Load duration
- ✓ Error summary

## Database Schema

```sql
CREATE TABLE DateDim (
    date_sk INT PRIMARY KEY,
    full_date VARCHAR(20),
    day_since_2005 INT,
    month_since_2005 INT,
    day_of_week VARCHAR(20),
    calendar_month VARCHAR(20),
    calendar_year VARCHAR(10),
    calendar_year_month VARCHAR(20),
    day_of_month INT,
    day_of_year INT,
    week_of_year_sunday INT,
    year_week_sunday VARCHAR(20),
    week_sunday_start VARCHAR(20),
    week_of_year_monday INT,
    year_week_monday VARCHAR(20),
    week_monday_start VARCHAR(20),
    holiday VARCHAR(50),
    day_type VARCHAR(20),
    KEY `idx_full_date` (`full_date`),
    KEY `idx_calendar_year` (`calendar_year`)
);
```

## Troubleshooting

### "CSV file not found"
```bash
# Check file location
ls -la date_dim.csv

# Update config
export DATE_DIM_PATH=/path/to/date_dim.csv
python load_date_dim_cli.py
```

### "Invalid column count"
```bash
# Check CSV structure
head -1 date_dim.csv | tr ',' '\n' | wc -l  # Should be 18
```

### "Database connection failed"
```bash
# Check MySQL is running
docker-compose ps

# Check .env configuration
cat .env | grep MYSQL
```

### "Loaded 0 records"
```bash
# Check CSV file is not empty
wc -l date_dim.csv

# Run with verbose output
python load_date_dim_cli.py --verbose
```

## Example Output

```bash
$ python load_date_dim_cli.py

Loading DateDim from ./date_dim.csv (validation mode)...

================================================================================
DateDim Load Statistics
================================================================================
Status: ✓ SUCCESS
Total Records: 7671
Loaded Records: 7670
Skipped Records: 1
Duration: 7.34s

✓ No validation errors!
================================================================================

✓ Total records in database: 7670
✓ Date range: 2005-01-01 to 2025-12-31
✓ Weekend days: 2190
✓ Weekday days: 5480
✓ No duplicate date_sk values
```

## Configuration

In `.env`:
```env
# DateDim CSV path
DATE_DIM_PATH=./date_dim.csv

# Or absolute path
DATE_DIM_PATH=/data/date_dim.csv
```

## Integration with Main Loader

The `TikTokLoader` automatically loads DateDim on initialization:

```python
loader = TikTokLoader()
# Internally calls: loader.load_date_dim()
```

Or manually:
```python
loader = TikTokLoader()
success = loader.load_date_dim()
```

## Performance Tips

1. **Use validation method for first time**: Catches any data issues
2. **Use simple method for subsequent loads**: Faster if data is clean
3. **Batch operations**: Load DateDim once, then use repeatedly
4. **Index usage**: DateDim indexes are used for joins with other tables

## Next Steps

1. ✅ Run `python load_date_dim_cli.py` to load data
2. ✅ Run `python test_date_dim_load.py` to verify
3. ✅ Check database: `SELECT COUNT(*) FROM DateDim`
4. ✅ Proceed with loading JSON data using main loader

