#!/usr/bin/env python3
"""
Script to ensure DateDim table is populated before starting main loader
This script runs at container startup to handle the case where init scripts didn't run
"""

import os
import sys
import time
import mysql.connector
from mysql.connector import Error
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection using environment variables"""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'db'),
            port=int(os.getenv('MYSQL_PORT', 3306)),
            user=os.getenv('MYSQL_USER', 'user'),
            password=os.getenv('MYSQL_PASSWORD', 'dwhtiktok'),
            database='dbStaging'
        )
        return connection
    except Error as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def check_date_dim_exists_and_populated():
    """Check if DateDim table exists and has data"""
    connection = get_db_connection()
    if not connection:
        return False, 0
    
    try:
        cursor = connection.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'dbStaging' AND table_name = 'DateDim'
        """)
        table_exists = cursor.fetchone()[0] > 0
        
        if not table_exists:
            logger.warning("DateDim table does not exist")
            return False, 0
        
        # Check row count
        cursor.execute("SELECT COUNT(*) FROM DateDim")
        row_count = cursor.fetchone()[0]
        
        logger.info(f"DateDim table exists with {row_count} rows")
        return True, row_count
        
    except Error as e:
        logger.error(f"Error checking DateDim: {e}")
        return False, 0
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def load_date_dim_from_csv():
    """Load DateDim data from CSV file"""
    csv_path = os.getenv('DATE_DIM_PATH', '/app/date_dim.csv')
    
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found at {csv_path}")
        return False
    
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # First, copy CSV to MySQL secure directory
        logger.info("Preparing to load DateDim data...")
        
        # Read CSV and insert row by row (safer than LOAD DATA INFILE in container)
        with open(csv_path, 'r') as f:
            lines = f.readlines()
        
        logger.info(f"Found {len(lines)} lines in CSV file")
        
        insert_query = """
            INSERT IGNORE INTO DateDim (
                date_sk, full_date, day_since_2005, month_since_2005, day_of_week, 
                calendar_month, calendar_year, calendar_year_month, day_of_month, 
                day_of_year, week_of_year_sunday, year_week_sunday, week_sunday_start,
                week_of_year_monday, year_week_monday, week_monday_start, 
                quarter, month, holiday, day_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        inserted_count = 0
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
                
            try:
                parts = line.split(',')
                if len(parts) != 20:
                    logger.warning(f"Skipping line {line_num}: expected 20 fields, got {len(parts)}")
                    continue
                
                # Convert data types
                data = [
                    int(parts[0]),      # date_sk
                    parts[1],           # full_date
                    int(parts[2]) if parts[2] else None,    # day_since_2005
                    int(parts[3]) if parts[3] else None,    # month_since_2005
                    parts[4],           # day_of_week
                    parts[5],           # calendar_month
                    int(parts[6]) if parts[6] else None,    # calendar_year
                    parts[7],           # calendar_year_month
                    int(parts[8]) if parts[8] else None,    # day_of_month
                    int(parts[9]) if parts[9] else None,    # day_of_year
                    int(parts[10]) if parts[10] else None,  # week_of_year_sunday
                    parts[11],          # year_week_sunday
                    parts[12],          # week_sunday_start
                    int(parts[13]) if parts[13] else None,  # week_of_year_monday
                    parts[14],          # year_week_monday
                    parts[15],          # week_monday_start
                    parts[16],          # quarter
                    int(parts[17]) if parts[17] else None,  # month
                    parts[18],          # holiday
                    parts[19]           # day_type
                ]
                
                cursor.execute(insert_query, data)
                inserted_count += 1
                
                if line_num % 1000 == 0:
                    connection.commit()
                    logger.info(f"Processed {line_num} lines...")
                    
            except (ValueError, Error) as e:
                logger.warning(f"Error processing line {line_num}: {e}")
                continue
        
        connection.commit()
        logger.info(f"Successfully loaded {inserted_count} rows into DateDim")
        return True
        
    except Error as e:
        logger.error(f"Error loading DateDim data: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def ensure_date_dim_ready():
    """Main function to ensure DateDim is ready"""
    logger.info("Starting DateDim readiness check...")
    
    # Wait for database to be ready
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        retry_count += 1
        logger.info(f"Checking database connection (attempt {retry_count}/{max_retries})")
        
        connection = get_db_connection()
        if connection:
            connection.close()
            break
        
        if retry_count < max_retries:
            logger.info("Database not ready, waiting 2 seconds...")
            time.sleep(2)
        else:
            logger.error("Could not connect to database after maximum retries")
            sys.exit(1)
    
    # Check DateDim status
    table_exists, row_count = check_date_dim_exists_and_populated()
    
    if table_exists and row_count > 0:
        logger.info(f"DateDim is ready with {row_count} rows")
        return True
    
    if not table_exists:
        logger.error("DateDim table does not exist. This should be created by init scripts.")
        logger.error("You may need to reset the database volume and restart containers.")
        sys.exit(1)
    
    if row_count == 0:
        logger.info("DateDim table exists but is empty. Loading data from CSV...")
        if load_date_dim_from_csv():
            logger.info("DateDim data loaded successfully")
            return True
        else:
            logger.error("Failed to load DateDim data")
            sys.exit(1)
    
    return False

if __name__ == "__main__":
    ensure_date_dim_ready()
    logger.info("DateDim readiness check completed successfully")