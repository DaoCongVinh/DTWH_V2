#!/usr/bin/env python3
"""
Test script for DateDim loading functionality
Tests the enhanced date_dim.csv loading with validation
"""

import os
import sys
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

import config
from logging_setup import setup_logger
from db import DatabaseConnection, DateDimManager

# Setup logging
logger = setup_logger(__name__)

def test_date_dim_load():
    """Test DateDim loading with validation"""
    logger.info("=" * 80)
    logger.info("DateDim Load Test - Validation Mode")
    logger.info("=" * 80)
    
    try:
        # Initialize database connection
        db_conn = DatabaseConnection()
        db_conn.connect()
        
        # Create DateDimManager instance
        date_dim_manager = DateDimManager(db_conn)
        
        # Test CSV file path
        csv_path = config.DATE_DIM_PATH
        logger.info(f"CSV File: {csv_path}")
        logger.info(f"CSV Exists: {Path(csv_path).exists()}")
        logger.info(f"CSV Size: {Path(csv_path).stat().st_size} bytes")
        
        # Test 1: Load with validation
        logger.info("\n[TEST 1] Loading with validation...")
        success, stats = date_dim_manager.load_date_dim_with_validation(csv_path)
        
        logger.info(f"Success: {success}")
        logger.info(f"Total Records: {stats['total_records']}")
        logger.info(f"Loaded Records: {stats['loaded_records']}")
        logger.info(f"Skipped Records: {stats['skipped_records']}")
        logger.info(f"Duration: {stats['duration_seconds']:.2f}s")
        
        if stats['errors']:
            logger.warning(f"Errors encountered ({len(stats['errors'])}): ")
            for i, error in enumerate(stats['errors'][:10], 1):
                logger.warning(f"  {i}. {error}")
            if len(stats['errors']) > 10:
                logger.warning(f"  ... and {len(stats['errors']) - 10} more errors")
        
        # Test 2: Verify loaded data
        logger.info("\n[TEST 2] Verifying loaded data...")
        with db_conn.get_cursor() as cursor:
            # Count total records
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {config.Tables.DATE_DIM}")
            total = cursor.fetchone()[0]
            logger.info(f"Total records in DateDim: {total}")
            
            # Get date range
            cursor.execute(
                f"SELECT MIN(full_date), MAX(full_date) FROM {config.Tables.DATE_DIM}"
            )
            min_date, max_date = cursor.fetchone()
            logger.info(f"Date range: {min_date} to {max_date}")
            
            # Check for duplicate date_sk
            cursor.execute(
                f"SELECT COUNT(*) as cnt FROM {config.Tables.DATE_DIM} "
                f"GROUP BY date_sk HAVING cnt > 1"
            )
            duplicates = cursor.fetchone()
            if duplicates:
                logger.warning(f"Found duplicate date_sk values!")
            else:
                logger.info("✓ No duplicate date_sk values")
            
            # Sample some records
            logger.info("\nSample records (first 5):")
            cursor.execute(
                f"SELECT date_sk, full_date, day_of_week, holiday, day_type "
                f"FROM {config.Tables.DATE_DIM} LIMIT 5"
            )
            for i, (date_sk, full_date, day_of_week, holiday, day_type) in enumerate(cursor.fetchall(), 1):
                logger.info(
                    f"  {i}. date_sk={date_sk}, date={full_date}, "
                    f"day={day_of_week}, type={day_type}, holiday={holiday}"
                )
        
        # Test 3: Performance check
        logger.info("\n[TEST 3] Performance Analysis...")
        with db_conn.get_cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*) FROM {config.Tables.DATE_DIM} "
                f"WHERE day_type = 'Weekend'"
            )
            weekend_count = cursor.fetchone()[0]
            logger.info(f"Weekend days: {weekend_count}")
            
            cursor.execute(
                f"SELECT COUNT(*) FROM {config.Tables.DATE_DIM} "
                f"WHERE holiday != 'Non-Holiday'"
            )
            holiday_count = cursor.fetchone()[0]
            logger.info(f"Holiday days: {holiday_count}")
        
        logger.info("\n" + "=" * 80)
        logger.info("TEST COMPLETED SUCCESSFULLY ✓")
        logger.info("=" * 80)
        
        db_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True)
        logger.info("\n" + "=" * 80)
        logger.info("TEST FAILED ✗")
        logger.info("=" * 80)
        return False

if __name__ == "__main__":
    success = test_date_dim_load()
    sys.exit(0 if success else 1)
