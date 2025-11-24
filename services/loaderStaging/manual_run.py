#!/usr/bin/env python3
"""
Manual Run Script for LoaderStaging
Usage: python manual_run.py [options]
       docker compose exec loader-staging python manual_run.py [options]
"""
import sys
from logging_setup import logger

if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info(">>> LOADER STAGING - MANUAL TRIGGER STARTED <<<")
    logger.info("=" * 70)
    
    try:
        # Import and run loader with command line arguments
        from loader import main
        main()
        
        logger.info("=" * 70)
        logger.info(">>> LOADER STAGING - MANUAL TRIGGER FINISHED <<<")
        logger.info("=" * 70)
    except Exception as e:
        logger.error(f"Manual run failed: {e}", exc_info=True)
        sys.exit(1)
