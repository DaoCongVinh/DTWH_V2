"""
Loader Staging Main Module
Orchestrates the entire ETL pipeline for TikTok data from APIFY crawler
"""

import json
import os
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Set, Tuple, Optional
import shutil
import csv
from io import StringIO

import jsonschema
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import config
from db import (
    DatabaseConnection,
    BatchFetcher,
    RawJsonManager,
    UpsertManager,
    LoadLogManager,
    DateDimManager
)

# ============================================================================
# Setup Logging
# ============================================================================

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Constants
# ============================================================================

BATCH_ID = f"LOAD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# ============================================================================
# Validator Class
# ============================================================================

class JSONValidator:
    """Validates JSON data against schema"""
    
    def __init__(self, schema_path: str):
        """
        Initialize validator
        
        Args:
            schema_path: Path to JSON schema file
        """
        self.schema = self._load_schema(schema_path)
    
    @staticmethod
    def _load_schema(schema_path: str) -> Dict[str, Any]:
        """Load JSON schema from file"""
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
            logger.info(f"Loaded schema from {schema_path}")
            return schema
        except Exception as e:
            logger.error(f"Error loading schema: {e}")
            raise
    
    def validate(self, data: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
        """
        Validate JSON data
        
        Args:
            data: JSON data to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            jsonschema.validate(instance=data, schema=self.schema)
            logger.info(f"JSON validation passed for {len(data)} items")
            return True, None
        except jsonschema.ValidationError as e:
            error_msg = f"JSON validation failed: {e.message} at path {'.'.join(str(p) for p in e.path)}"
            logger.error(error_msg)
            return False, error_msg
        except jsonschema.SchemaError as e:
            error_msg = f"Schema error: {e.message}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected validation error: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

# ============================================================================
# Data Transformer Class
# ============================================================================

class DataTransformer:
    """Transforms raw JSON data into structured staging tables"""
    
    @staticmethod
    def extract_author(item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract author data from item"""
        author_meta = item.get("authorMeta", {})
        return {
            "author_id": author_meta.get("id", ""),
            "author_name": author_meta.get("name", ""),
            "avatar": author_meta.get("avatar", ""),
        }
    
    @staticmethod
    def extract_video(item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract video data from item"""
        video_meta = item.get("videoMeta", {})
        create_time = datetime.fromtimestamp(
            item.get("createTime", 0)
        ).strftime('%Y-%m-%d %H:%M:%S') if item.get("createTime") else None
        
        return {
            "video_id": item.get("id", ""),
            "author_id": item.get("authorMeta", {}).get("id", ""),
            "text_content": item.get("text", ""),
            "duration": video_meta.get("duration", 0),
            "create_time": create_time,
            "web_video_url": item.get("webVideoUrl", ""),
        }
    
    @staticmethod
    def extract_interaction(item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract interaction/stats data from item"""
        return {
            "video_id": item.get("id", ""),
            "digg_count": item.get("diggCount", 0),
            "play_count": item.get("playCount", 0),
            "share_count": item.get("shareCount", 0),
            "comment_count": item.get("commentCount", 0),
            "collect_count": item.get("collectCount", 0),
        }
    
    @staticmethod
    def transform_file(json_data: List[Dict[str, Any]]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """
        Transform JSON file into 3 tables: Authors, Videos, Interactions
        
        Args:
            json_data: Raw JSON data
            
        Returns:
            Tuple of (authors, videos, interactions)
        """
        authors = []
        videos = []
        interactions = []
        
        for item in json_data:
            authors.append(DataTransformer.extract_author(item))
            videos.append(DataTransformer.extract_video(item))
            interactions.append(DataTransformer.extract_interaction(item))
        
        return authors, videos, interactions

# ============================================================================
# Main Loader Class
# ============================================================================

class TikTokLoader:
    """Main loader orchestrator"""
    
    def __init__(self):
        """Initialize loader"""
        logger.info("Initializing TikTok Loader...")
        self.db_conn = DatabaseConnection()
        self.db_conn.connect()
        
        self.batch_fetcher = BatchFetcher(self.db_conn)
        self.raw_json_manager = RawJsonManager(self.db_conn)
        self.upsert_manager = UpsertManager(self.db_conn)
        self.load_log_manager = LoadLogManager(self.db_conn)
        self.date_dim_manager = DateDimManager(self.db_conn)
        
        self.validator = JSONValidator(config.SCHEMA_FILE)
        self.transformer = DataTransformer()
        
        # Load cached data for optimization
        logger.info("Loading batch data...")
        self.existing_authors, self.existing_videos, self.existing_interactions = self.batch_fetcher.fetch_all()
        
        # Get today's date_sk
        self.today_date_sk = self.batch_fetcher.get_today_date_sk()
        if not self.today_date_sk:
            logger.error("Failed to get today's date_sk. Aborting.")
            raise RuntimeError("Cannot get today's date_sk from DateDim")
        
        logger.info("Loader initialized successfully")
    
    def load_raw_json(self, file_path: str, content: str) -> bool:
        """
        Load raw JSON file content
        
        Args:
            file_path: Path to JSON file
            content: File content
            
        Returns:
            bool: True if successful
        """
        filename = os.path.basename(file_path)
        logger.info(f"Saving raw JSON: {filename}")
        
        return self.raw_json_manager.insert_raw_json(
            content=content,
            filename=filename,
            status=config.LOAD_STATUS_SUCCESS
        )
    
    def load_raw_json_failed(self, file_path: str, error_message: str) -> bool:
        """
        Log failed JSON load
        
        Args:
            file_path: Path to JSON file
            error_message: Error details
            
        Returns:
            bool: True if successful
        """
        filename = os.path.basename(file_path)
        logger.info(f"Saving failed raw JSON: {filename}")
        
        return self.raw_json_manager.insert_raw_json(
            content="",  # Don't store content for failed files
            filename=filename,
            status=config.LOAD_STATUS_FAILED,
            error_message=error_message
        )
    
    def process_staging_tables(
        self,
        authors: List[Dict[str, Any]],
        videos: List[Dict[str, Any]],
        interactions: List[Dict[str, Any]],
        batch_id: str,
        source_filename: str
    ) -> Dict[str, Any]:
        """
        Process and upsert data into staging tables
        
        Args:
            authors: Author records
            videos: Video records
            interactions: Interaction records
            batch_id: Batch identifier
            source_filename: Source file name
            
        Returns:
            Dict with load results
        """
        results = {
            "authors": {"inserted": 0, "updated": 0, "skipped": 0, "failed": 0},
            "videos": {"inserted": 0, "updated": 0, "skipped": 0, "failed": 0},
            "interactions": {"inserted": 0, "updated": 0, "skipped": 0, "failed": 0},
        }
        
        # Process Authors
        logger.info(f"Processing {len(authors)} authors...")
        start_time = datetime.now()
        for author in authors:
            if not author["author_id"] or not author["author_name"]:
                results["authors"]["failed"] += 1
                continue
            
            success, action = self.upsert_manager.upsert_author(
                author_id=author["author_id"],
                author_name=author["author_name"],
                avatar=author["avatar"],
                date_sk=self.today_date_sk,
                existing_authors=self.existing_authors
            )
            
            if success:
                if action == "INSERT":
                    results["authors"]["inserted"] += 1
                    self.existing_authors.add(author["author_id"])
                elif action == "UPDATE":
                    results["authors"]["updated"] += 1
                elif action == "SKIP":
                    results["authors"]["skipped"] += 1
            else:
                results["authors"]["failed"] += 1
        
        end_time = datetime.now()
        self.load_log_manager.log_load(
            batch_id=batch_id,
            table_name=config.Tables.AUTHORS,
            record_count=len(authors),
            inserted_count=results["authors"]["inserted"],
            updated_count=results["authors"]["updated"],
            skipped_count=results["authors"]["skipped"],
            status=config.LOAD_STATUS_SUCCESS if results["authors"]["failed"] == 0 else config.LOAD_STATUS_PARTIAL,
            start_time=start_time,
            end_time=end_time,
            source_filename=source_filename
        )
        
        # Process Videos
        logger.info(f"Processing {len(videos)} videos...")
        start_time = datetime.now()
        for video in videos:
            if not video["video_id"] or not video["author_id"]:
                results["videos"]["failed"] += 1
                continue
            
            success, action = self.upsert_manager.upsert_video(
                video_id=video["video_id"],
                author_id=video["author_id"],
                text_content=video["text_content"],
                duration=video["duration"],
                create_time=video["create_time"],
                web_video_url=video["web_video_url"],
                date_sk=self.today_date_sk,
                existing_videos=self.existing_videos
            )
            
            if success:
                if action == "INSERT":
                    results["videos"]["inserted"] += 1
                    self.existing_videos.add(video["video_id"])
                elif action == "UPDATE":
                    results["videos"]["updated"] += 1
                elif action == "SKIP":
                    results["videos"]["skipped"] += 1
            else:
                results["videos"]["failed"] += 1
        
        end_time = datetime.now()
        self.load_log_manager.log_load(
            batch_id=batch_id,
            table_name=config.Tables.VIDEOS,
            record_count=len(videos),
            inserted_count=results["videos"]["inserted"],
            updated_count=results["videos"]["updated"],
            skipped_count=results["videos"]["skipped"],
            status=config.LOAD_STATUS_SUCCESS if results["videos"]["failed"] == 0 else config.LOAD_STATUS_PARTIAL,
            start_time=start_time,
            end_time=end_time,
            source_filename=source_filename
        )
        
        # Process VideoInteractions
        logger.info(f"Processing {len(interactions)} interactions...")
        start_time = datetime.now()
        for interaction in interactions:
            if not interaction["video_id"]:
                results["interactions"]["failed"] += 1
                continue
            
            success, action = self.upsert_manager.upsert_interaction(
                video_id=interaction["video_id"],
                digg_count=interaction["digg_count"],
                play_count=interaction["play_count"],
                share_count=interaction["share_count"],
                comment_count=interaction["comment_count"],
                collect_count=interaction["collect_count"],
                date_sk=self.today_date_sk,
                existing_interactions=self.existing_interactions
            )
            
            if success:
                if action == "INSERT":
                    results["interactions"]["inserted"] += 1
                    self.existing_interactions.add(interaction["video_id"])
                elif action == "UPDATE":
                    results["interactions"]["updated"] += 1
                elif action == "SKIP":
                    results["interactions"]["skipped"] += 1
            else:
                results["interactions"]["failed"] += 1
        
        end_time = datetime.now()
        self.load_log_manager.log_load(
            batch_id=batch_id,
            table_name=config.Tables.VIDEO_INTERACTIONS,
            record_count=len(interactions),
            inserted_count=results["interactions"]["inserted"],
            updated_count=results["interactions"]["updated"],
            skipped_count=results["interactions"]["skipped"],
            status=config.LOAD_STATUS_SUCCESS if results["interactions"]["failed"] == 0 else config.LOAD_STATUS_PARTIAL,
            start_time=start_time,
            end_time=end_time,
            source_filename=source_filename
        )
        
        return results
    
    def move_file(self, file_path: str, status: str) -> bool:
        """
        Move file to processed or failed directory
        
        Args:
            file_path: Path to file
            status: Status (SUCCESS/FAILED)
            
        Returns:
            bool: True if successful
        """
        try:
            filename = os.path.basename(file_path)
            if status == config.LOAD_STATUS_SUCCESS:
                dest_dir = config.PROCESSED_DIR
            else:
                dest_dir = config.FAILED_DIR
            
            dest_path = os.path.join(dest_dir, filename)
            shutil.move(file_path, dest_path)
            logger.info(f"Moved {filename} to {dest_dir}")
            return True
        except Exception as e:
            logger.error(f"Error moving file: {e}")
            return False
    
    def process_file(self, file_path: str, skip_staging: bool = False, keep_file: bool = False) -> bool:
        """
        Process a single JSON file
        
        Args:
            file_path: Path to JSON file
            skip_staging: Skip staging table processing
            keep_file: Keep file after processing
            
        Returns:
            bool: True if successful
        """
        filename = os.path.basename(file_path)
        logger.info(f"Processing file: {filename}")
        
        try:
            # Read JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                json_data = json.loads(content)
            
            # Validate JSON
            is_valid, error_msg = self.validator.validate(json_data)
            if not is_valid:
                logger.error(f"Validation failed: {error_msg}")
                self.load_raw_json_failed(file_path, error_msg)
                if not keep_file:
                    self.move_file(file_path, config.LOAD_STATUS_FAILED)
                return False
            
            # Load raw JSON (always)
            if not self.load_raw_json(file_path, content):
                logger.error("Failed to save raw JSON")
                if not keep_file:
                    self.move_file(file_path, config.LOAD_STATUS_FAILED)
                return False
            
            # Skip staging if requested
            if skip_staging:
                logger.info("Staging processing skipped")
                if not keep_file:
                    self.move_file(file_path, config.LOAD_STATUS_SUCCESS)
                return True
            
            # Transform data
            authors, videos, interactions = self.transformer.transform_file(json_data)
            
            # Process staging tables
            results = self.process_staging_tables(
                authors=authors,
                videos=videos,
                interactions=interactions,
                batch_id=BATCH_ID,
                source_filename=filename
            )
            
            logger.info(f"File processed: {results}")
            
            # Move file
            if not keep_file:
                self.move_file(file_path, config.LOAD_STATUS_SUCCESS)
            
            return True
            
        except json.JSONDecodeError as e:
            error_msg = f"JSON decode error: {str(e)}"
            logger.error(error_msg)
            self.load_raw_json_failed(file_path, error_msg)
            if not keep_file:
                self.move_file(file_path, config.LOAD_STATUS_FAILED)
            return False
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(error_msg)
            self.load_raw_json_failed(file_path, error_msg)
            if not keep_file:
                self.move_file(file_path, config.LOAD_STATUS_FAILED)
            return False
    
    def process_directory(self, skip_staging: bool = False, keep_files: bool = False) -> Dict[str, Any]:
        """
        Process all JSON files in storage directory
        
        Args:
            skip_staging: Skip staging table processing
            keep_files: Keep files after processing
            
        Returns:
            Dict with summary statistics
        """
        logger.info(f"Starting directory scan: {config.STORAGE_PATH}")
        
        stats = {
            "total_files": 0,
            "successful_files": 0,
            "failed_files": 0,
            "skipped_files": 0,
        }
        
        json_files = list(Path(config.STORAGE_PATH).glob(config.JSON_FILE_PATTERN))
        json_files = [f for f in json_files if f.is_file()]
        
        logger.info(f"Found {len(json_files)} JSON files")
        
        for json_file in json_files:
            stats["total_files"] += 1
            if self.process_file(str(json_file), skip_staging=skip_staging, keep_file=keep_files):
                stats["successful_files"] += 1
            else:
                stats["failed_files"] += 1
        
        logger.info(f"Directory processing complete: {stats}")
        return stats
    
    def load_date_dim(self) -> bool:
        """
        Load date dimension table from date_dim.csv
        
        Uses validation-based loading with detailed error reporting
        
        Returns:
            bool: True if load successful, False otherwise
        """
        logger.info("Loading DateDim table from CSV...")
        
        # Try validation load first (with detailed error handling)
        success, stats = self.date_dim_manager.load_date_dim_with_validation(config.DATE_DIM_PATH)
        
        if success:
            logger.info(
                f"DateDim load completed successfully: "
                f"Total={stats['total_records']}, "
                f"Loaded={stats['loaded_records']}, "
                f"Skipped={stats['skipped_records']}, "
                f"Duration={stats['duration_seconds']:.2f}s"
            )
            if stats['errors']:
                logger.warning(f"Encountered {len(stats['errors'])} validation warnings during load")
                for error in stats['errors'][:5]:  # Log first 5 errors
                    logger.warning(f"  - {error}")
        else:
            logger.error(
                f"DateDim load failed: "
                f"Total records processed={stats['total_records']}, "
                f"Loaded={stats['loaded_records']}, "
                f"Errors={len(stats['errors'])}"
            )
            for error in stats['errors'][:10]:  # Log first 10 errors
                logger.error(f"  - {error}")
        
        return success
    
    def cleanup(self):
        """Cleanup and close connections"""
        logger.info("Cleaning up...")
        if self.db_conn.is_connected():
            self.db_conn.disconnect()

# ============================================================================
# Scheduler
# ============================================================================

class LoaderScheduler:
    """Manages scheduled loader runs"""
    
    def __init__(self, loader: TikTokLoader):
        """
        Initialize scheduler
        
        Args:
            loader: TikTok loader instance
        """
        self.loader = loader
        self.scheduler = BackgroundScheduler(config.SCHEDULER_CONFIG)
    
    def start(self, cron_expression: str):
        """
        Start scheduler with cron expression
        
        Args:
            cron_expression: Cron expression (e.g., "0 */1 * * *")
        """
        trigger = CronTrigger.from_crontab(cron_expression, timezone="Asia/Ho_Chi_Minh")
        self.scheduler.add_job(
            self.loader.process_directory,
            trigger=trigger,
            id="loader_job",
            name="TikTok Loader Job",
            replace_existing=True
        )
        self.scheduler.start()
        logger.info(f"Scheduler started with cron: {cron_expression}")
    
    def stop(self):
        """Stop scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")

# ============================================================================
# CLI Interface
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="TikTok Loader Staging Service")
    parser.add_argument("--load_raw", action="store_true", help="Only load raw JSON")
    parser.add_argument("--load_staging", action="store_true", help="Only load staging tables")
    parser.add_argument("--no-remove", action="store_true", help="Don't move files after processing")
    parser.add_argument("--schedule", action="store_true", help="Run with scheduler")
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("TikTok Loader Staging Service Started")
    logger.info("=" * 80)
    
    try:
        # Initialize loader
        loader = TikTokLoader()
        
        # Load DateDim if needed
        loader.load_date_dim()
        
        # Determine skip staging mode
        skip_staging = args.load_raw  # Skip staging if only loading raw
        keep_files = args.no_remove
        
        # Run scheduler or process directory
        if args.schedule:
            scheduler = LoaderScheduler(loader)
            scheduler.start(config.LOADER_SCHEDULE_CRON)
            logger.info("Scheduler mode active. Press Ctrl+C to stop.")
            try:
                while True:
                    import time
                    time.sleep(1)
            except KeyboardInterrupt:
                scheduler.stop()
                logger.info("Scheduler stopped by user")
        else:
            # Single run
            stats = loader.process_directory(skip_staging=skip_staging, keep_files=keep_files)
            logger.info(f"Final stats: {stats}")
        
        loader.cleanup()
        logger.info("Loader finished successfully")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
