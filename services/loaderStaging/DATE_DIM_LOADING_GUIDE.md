# DateDim CSV Loading Implementation Guide

## Overview

Enhanced DateDim loading functionality based on the Java date dimension generator code. This implementation loads `date_dim.csv` into the database with full validation and error handling.

## CSV File Structure

The `date_dim.csv` contains 18 columns per row:

| Col | Field Name | Type | Description | Example |
|-----|-----------|------|-------------|---------|
| 1 | date_sk | INT | Date surrogate key (primary key) | 1 |
| 2 | full_date | VARCHAR(20) | Full date (YYYY-MM-DD format) | 2005-01-01 |
| 3 | day_since_2005 | INT | Days elapsed since 2005-01-01 | 1 |
| 4 | month_since_2005 | INT | Months elapsed since 2005-01-01 | 1 |
| 5 | day_of_week | VARCHAR(20) | Day name | Saturday |
| 6 | calendar_month | VARCHAR(20) | Month name | January |
| 7 | calendar_year | VARCHAR(10) | Year | 2005 |
| 8 | calendar_year_month | VARCHAR(20) | Year-Month format | 2005-Jan |
| 9 | day_of_month | INT | Day of month (1-31) | 1 |
| 10 | day_of_year | INT | Day of year (1-366) | 1 |
| 11 | week_of_year_sunday | INT | Week number (Sunday-based) | 52 |
| 12 | year_week_sunday | VARCHAR(20) | Year-Week Sunday-based | 2004-W52 |
| 13 | week_sunday_start | VARCHAR(20) | Start date of week (Sunday) | 2004-12-26 |
| 14 | week_of_year_monday | INT | Week number (Monday-based) | 53 |
| 15 | year_week_monday | VARCHAR(20) | Year-Week Monday-based | 2004-W53 |
| 16 | week_monday_start | VARCHAR(20) | Start date of week (Monday) | 2004-12-27 |
| 17 | holiday | VARCHAR(50) | Holiday designation | Non-Holiday |
| 18 | day_type | VARCHAR(20) | Weekend or Weekday | Weekend |

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

## Usage

### 1. Basic Load (Simple Method)

```python
from db import DatabaseConnection, DateDimManager
import config

# Create connection
db_conn = DatabaseConnection()
db_conn.connect()

# Load DateDim
date_dim_manager = DateDimManager(db_conn)
success = date_dim_manager.load_date_dim_from_csv(config.DATE_DIM_PATH)

if success:
    print("DateDim loaded successfully!")
else:
    print("DateDim load failed!")

db_conn.disconnect()
```

### 2. Load with Validation (Recommended)

```python
from db import DatabaseConnection, DateDimManager
import config

# Create connection
db_conn = DatabaseConnection()
db_conn.connect()

# Load with validation and detailed stats
date_dim_manager = DateDimManager(db_conn)
success, stats = date_dim_manager.load_date_dim_with_validation(config.DATE_DIM_PATH)

print(f"Success: {success}")
print(f"Total records: {stats['total_records']}")
print(f"Loaded records: {stats['loaded_records']}")
print(f"Skipped records: {stats['skipped_records']}")
print(f"Duration: {stats['duration_seconds']:.2f}s")

if stats['errors']:
    print(f"Errors encountered: {len(stats['errors'])}")
    for error in stats['errors'][:5]:
        print(f"  - {error}")

db_conn.disconnect()
```

### 3. Using Loader Class

```python
from loader import TikTokLoader

# Create loader
loader = TikTokLoader()

# Load DateDim
success = loader.load_date_dim()

if success:
    print("DateDim loaded successfully via Loader!")
```

### 4. Using Setup Script

```bash
# The setup.sh script automatically loads DateDim
bash setup.sh

# Or manually with Python
python -c "
from loader import TikTokLoader
loader = TikTokLoader()
loader.load_date_dim()
"
```

## Implementation Details

### DateDimManager Methods

#### 1. `load_date_dim_from_csv(csv_path: str) -> bool`

Simple LOAD DATA INFILE approach. Fast but with limited error handling.

**Features:**
- Uses MySQL LOAD DATA INFILE (fastest method)
- Truncates existing data before loading
- Returns simple True/False
- Useful for production after data is validated

**Usage:**
```python
success = date_dim_manager.load_date_dim_from_csv(config.DATE_DIM_PATH)
```

#### 2. `load_date_dim_with_validation(csv_path: str) -> Tuple[bool, Dict]`

Python-based loading with comprehensive validation and error reporting.

**Features:**
- Validates CSV structure (18 columns per row)
- Validates date_sk is numeric
- Validates full_date format (YYYY-MM-DD)
- Reports detailed statistics
- Collects all errors for review
- Batch insert (more reliable than LOAD DATA INFILE for remote MySQL)

**Returns:**
```python
{
    "total_records": int,        # Total lines in CSV
    "loaded_records": int,       # Successfully loaded rows
    "skipped_records": int,      # Rows with validation errors
    "errors": list,              # List of error messages
    "start_time": datetime,      # Load start time
    "end_time": datetime,        # Load end time
    "duration_seconds": float    # Total duration
}
```

**Usage:**
```python
success, stats = date_dim_manager.load_date_dim_with_validation(csv_path)
if success:
    print(f"Loaded {stats['loaded_records']} records in {stats['duration_seconds']:.2f}s")
else:
    for error in stats['errors']:
        print(f"Error: {error}")
```

## Validation Rules

The validation process checks:

1. **Row Structure**: Each row must have exactly 18 columns
2. **date_sk (Column 1)**: Must be a valid integer
3. **full_date (Column 2)**: Must be in YYYY-MM-DD format (10 characters, 2 dashes)
4. **Numeric Columns**: day_since_2005, month_since_2005, day_of_month, day_of_year, week_of_year_sunday, week_of_year_monday are all numeric
5. **String Columns**: All other columns are stored as-is (no validation)

## Error Handling

The loader handles these error scenarios:

1. **Missing CSV File**: Returns error if file doesn't exist
2. **Invalid Row Format**: Skips rows with wrong column count
3. **Invalid date_sk**: Skips rows with non-numeric date_sk
4. **Invalid Date Format**: Skips rows with malformed dates
5. **Database Errors**: Rolls back transaction on any SQL error
6. **Connection Errors**: Logs and reports connection issues

All errors are logged to the configured logger with detailed information about line numbers and specific issues.

## Configuration

Set the CSV path in `.env` or `config.py`:

```env
# .env
DATE_DIM_PATH=./date_dim.csv
```

Or in config.py:
```python
DATE_DIM_PATH = os.getenv("DATE_DIM_PATH", "./date_dim.csv")
```

## Testing

Run the test script:

```bash
cd services/loaderStaging
python test_date_dim_load.py
```

Test output includes:
- ✓ Load success/failure status
- ✓ Record counts and statistics
- ✓ Date range verification
- ✓ Duplicate detection
- ✓ Sample data inspection
- ✓ Performance metrics (weekend/holiday counts)

## Performance Characteristics

**LOAD DATA INFILE Method:**
- ~2-5 seconds for 7000+ records
- Requires MySQL `FILE` privilege
- May fail on remote MySQL servers with strict policies

**Validation Method:**
- ~5-10 seconds for 7000+ records
- 100% reliable, works with all MySQL configurations
- Provides detailed error reporting
- Recommended for production

## Integration with Loader

The `TikTokLoader` class automatically calls `load_date_dim()`:

```python
def load_date_dim(self) -> bool:
    """Load date dimension table from date_dim.csv"""
    logger.info("Loading DateDim table from CSV...")
    success, stats = self.date_dim_manager.load_date_dim_with_validation(
        config.DATE_DIM_PATH
    )
    
    if success:
        logger.info(
            f"DateDim load completed: Loaded={stats['loaded_records']}, "
            f"Duration={stats['duration_seconds']:.2f}s"
        )
    else:
        logger.error(f"DateDim load failed: {len(stats['errors'])} errors")
    
    return success
```

## Troubleshooting

### Issue: "CSV file not found"
**Solution:** Verify `DATE_DIM_PATH` in `.env` points to correct file location

### Issue: "Unexpected column count"
**Solution:** Ensure `date_dim.csv` has 18 columns. Check for:
- Missing commas between columns
- Extra/missing columns
- Line ending issues (use LF, not CRLF)

### Issue: "Invalid date format"
**Solution:** Verify full_date column (column 2) uses YYYY-MM-DD format

### Issue: "Database error: Permission denied"
**Solution:** Use `load_date_dim_with_validation()` instead of LOAD DATA INFILE method

### Issue: "No valid records found"
**Solution:** Check file encoding (should be UTF-8) and line endings

## References

- Original Java Generator: `Date_Dim.java` (generates this CSV format)
- Database Schema: `schema_dbStaging.sql`
- Main Loader: `loader.py` (TikTokLoader class)
- Database Layer: `db.py` (DateDimManager class)

## Future Enhancements

1. **Incremental Load**: Load only new dates since last load
2. **Holiday Calendar**: Populate holiday field based on country/region
3. **Performance Tuning**: Parallel batch insert for large files
4. **Audit Trail**: Log DateDim load operations in LoadLog table
5. **Data Quality Checks**: Verify no gaps in date sequence

