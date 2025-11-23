#!/usr/bin/env python3
"""
DateDim Load CLI - Simple command-line interface for loading date_dim.csv
"""

import sys
import logging
from pathlib import Path
from argparse import ArgumentParser

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from db import DatabaseConnection, DateDimManager
from logging_setup import setup_logging
import config

setup_logging()
logger = logging.getLogger(__name__)

def load_date_dim_simple(csv_path: str) -> bool:
    """
    Load DateDim with simple LOAD DATA INFILE (fastest)
    
    Args:
        csv_path: Path to date_dim.csv
        
    Returns:
        bool: Success status
    """
    logger.info(f"Loading DateDim from {csv_path} (simple mode)...")
    
    try:
        db_conn = DatabaseConnection()
        db_conn.connect()
        date_dim_manager = DateDimManager(db_conn)
        
        success = date_dim_manager.load_date_dim_from_csv(csv_path)
        
        db_conn.disconnect()
        return success
    except Exception as e:
        logger.error(f"Load failed: {e}")
        return False

def load_date_dim_validated(csv_path: str, verbose: bool = False) -> bool:
    """
    Load DateDim with validation (recommended)
    
    Args:
        csv_path: Path to date_dim.csv
        verbose: Print detailed statistics
        
    Returns:
        bool: Success status
    """
    logger.info(f"Loading DateDim from {csv_path} (validation mode)...")
    
    try:
        db_conn = DatabaseConnection()
        db_conn.connect()
        date_dim_manager = DateDimManager(db_conn)
        
        success, stats = date_dim_manager.load_date_dim_with_validation(csv_path)
        
        # Print statistics
        print("\n" + "=" * 80)
        print("DateDim Load Statistics")
        print("=" * 80)
        print(f"Status: {'✓ SUCCESS' if success else '✗ FAILED'}")
        print(f"Total Records: {stats['total_records']}")
        print(f"Loaded Records: {stats['loaded_records']}")
        print(f"Skipped Records: {stats['skipped_records']}")
        print(f"Duration: {stats['duration_seconds']:.2f}s")
        
        if stats['errors']:
            print(f"\nErrors Encountered: {len(stats['errors'])}")
            if verbose:
                for i, error in enumerate(stats['errors'][:20], 1):
                    print(f"  {i}. {error}")
                if len(stats['errors']) > 20:
                    print(f"  ... and {len(stats['errors']) - 20} more errors")
            else:
                for i, error in enumerate(stats['errors'][:5], 1):
                    print(f"  {i}. {error}")
                if len(stats['errors']) > 5:
                    print(f"  ... and {len(stats['errors']) - 5} more errors")
                print("\n  Use --verbose for all errors")
        else:
            print("\n✓ No validation errors!")
        
        print("=" * 80 + "\n")
        
        db_conn.disconnect()
        return success
    except Exception as e:
        logger.error(f"Load failed: {e}")
        return False

def verify_date_dim(csv_path: str) -> bool:
    """
    Verify DateDim was loaded correctly
    
    Args:
        csv_path: Path to date_dim.csv
        
    Returns:
        bool: Verification success
    """
    logger.info("Verifying DateDim load...")
    
    try:
        db_conn = DatabaseConnection()
        db_conn.connect()
        
        with db_conn.get_cursor() as cursor:
            # Count records
            cursor.execute("SELECT COUNT(*) as cnt FROM DateDim")
            total = cursor.fetchone()[0]
            print(f"✓ Total records in database: {total}")
            
            # Get date range
            cursor.execute("SELECT MIN(full_date), MAX(full_date) FROM DateDim")
            min_date, max_date = cursor.fetchone()
            print(f"✓ Date range: {min_date} to {max_date}")
            
            # Sample records
            cursor.execute("SELECT COUNT(*) FROM DateDim WHERE day_type = 'Weekend'")
            weekend_count = cursor.fetchone()[0]
            print(f"✓ Weekend days: {weekend_count}")
            
            cursor.execute("SELECT COUNT(*) FROM DateDim WHERE day_type = 'Weekday'")
            weekday_count = cursor.fetchone()[0]
            print(f"✓ Weekday days: {weekday_count}")
            
            # Check for duplicates
            cursor.execute(
                "SELECT COUNT(*) FROM (SELECT date_sk FROM DateDim GROUP BY date_sk HAVING COUNT(*) > 1) t"
            )
            duplicates = cursor.fetchone()[0]
            if duplicates > 0:
                print(f"✗ Found {duplicates} duplicate date_sk values!")
                return False
            else:
                print("✓ No duplicate date_sk values")
        
        db_conn.disconnect()
        return True
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return False

def main():
    """Main CLI entry point"""
    parser = ArgumentParser(
        description="Load date_dim.csv into DateDim table",
        epilog="Examples:\n"
               "  python load_date_dim_cli.py                    # Load with validation\n"
               "  python load_date_dim_cli.py --simple           # Load with LOAD DATA INFILE\n"
               "  python load_date_dim_cli.py --verify           # Verify loaded data\n"
               "  python load_date_dim_cli.py --verbose          # Show all errors"
    )
    
    parser.add_argument(
        "--simple",
        action="store_true",
        help="Use simple LOAD DATA INFILE method (faster, less validation)"
    )
    
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify DateDim was loaded correctly"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed error information"
    )
    
    parser.add_argument(
        "--csv",
        default=config.DATE_DIM_PATH,
        help=f"Path to date_dim.csv (default: {config.DATE_DIM_PATH})"
    )
    
    args = parser.parse_args()
    
    # Verify CSV exists
    csv_path = Path(args.csv)
    if not csv_path.exists():
        logger.error(f"CSV file not found: {args.csv}")
        return 1
    
    # Just verify
    if args.verify:
        success = verify_date_dim(args.csv)
        return 0 if success else 1
    
    # Load
    if args.simple:
        success = load_date_dim_simple(args.csv)
    else:
        success = load_date_dim_validated(args.csv, args.verbose)
    
    if success:
        # Verify after load
        logger.info("Verifying load...")
        verify_success = verify_date_dim(args.csv)
        return 0 if verify_success else 1
    else:
        logger.error("Load failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
