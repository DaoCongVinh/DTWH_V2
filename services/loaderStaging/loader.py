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
    
)
from transformer import TikTokTransformer

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BATCH_ID = f"LOAD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

class JSONValidator:
    """Validate JSON data against schema"""
    
    def __init__(self, schema_path: str):
        self.schema = self._load_schema(schema_path)
    
    @staticmethod
    def _load_schema(schema_path: str) -> Dict[str, Any]:
        """Load JSON schema from file"""
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
            logger.info(f"Đã tải schema từ {schema_path}")
            return schema
        except Exception as e:
            logger.error(f"Lỗi khi tải schema: {e}")
            raise
    
    def validate(self, data: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
        """Validate JSON data against schema"""
        try:
            jsonschema.validate(instance=data, schema=self.schema)
            logger.info(f"Kiểm tra JSON thành công cho {len(data)} items")
            return True, None
        except jsonschema.ValidationError as e:
            error_msg = f"Kiểm tra JSON thất bại: {e.message} tại đường dẫn {'.'.join(str(p) for p in e.path)}"
            logger.error(error_msg)
            return False, error_msg
        except jsonschema.SchemaError as e:
            error_msg = f"Lỗi schema: {e.message}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Lỗi kiểm tra không mong đợi: {str(e)}"
            logger.error(error_msg)
            return False, error_msg



class TikTokLoader:
    """Main ETL orchestrator for TikTok data loading"""
    
    def __init__(self):
        logger.info("Đang khởi tạo TikTok Loader...")
        
        self.db_conn = DatabaseConnection()
        self.db_conn.connect()
        
        self.batch_fetcher = BatchFetcher(self.db_conn)
        self.raw_json_manager = RawJsonManager(self.db_conn)
        self.upsert_manager = UpsertManager(self.db_conn)
        self.load_log_manager = LoadLogManager(self.db_conn)
        
        self.validator = JSONValidator(config.SCHEMA_FILE)
        
        logger.info("Đang tải dữ liệu batch...")
        self.existing_authors, self.existing_videos, self.existing_interactions = self.batch_fetcher.fetch_all()
        
        self.today_date_sk = self.batch_fetcher.get_today_date_sk()
        if not self.today_date_sk:
            logger.error("Không lấy được date_sk hôm nay. Dừng xử lý.")
            raise RuntimeError("Không thể lấy date_sk hôm nay từ DateDim")
        
        logger.info("Khởi tạo Loader thành công")
    
    def load_raw_json(self, file_path: str, content: str) -> bool:
        """Save raw JSON to database with deduplication"""
        filename = os.path.basename(file_path)
        logger.info(f"Đang lưu raw JSON: {filename}")
        
        return self.raw_json_manager.insert_raw_json(
            content=content,
            filename=filename,
            status=config.LOAD_STATUS_SUCCESS
        )
    
    def load_raw_json_failed(self, file_path: str, error_message: str) -> bool:
        """Log failed JSON file to database"""
        filename = os.path.basename(file_path)
        logger.info(f"Đang ghi nhận file lỗi: {filename}")
        
        return self.raw_json_manager.insert_raw_json(
            content="",
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
        results = {
            "authors": {"inserted": 0, "updated": 0, "skipped": 0, "failed": 0},
            "videos": {"inserted": 0, "updated": 0, "skipped": 0, "failed": 0},
            "interactions": {"inserted": 0, "updated": 0, "skipped": 0, "failed": 0},
        }
        
        # Process Authors
        logger.info(f"Đang xử lý {len(authors)} tác giả...")
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
        logger.info(f"Đang xử lý {len(videos)} video...")
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
        logger.info(f"Đang xử lý {len(interactions)} tương tác...")
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
        """Move processed file to appropriate directory"""
        try:
            filename = os.path.basename(file_path)
            dest_dir = config.PROCESSED_DIR if status == config.LOAD_STATUS_SUCCESS else config.FAILED_DIR
            dest_path = os.path.join(dest_dir, filename)
            shutil.move(file_path, dest_path)
            logger.info(f"Đã di chuyển {filename} tới {dest_dir}")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi di chuyển file: {e}")
            return False
    
    def process_file(self, file_path: str, skip_staging: bool = False, keep_file: bool = False) -> bool:
        """Process single JSON file through ETL pipeline"""
        filename = os.path.basename(file_path)
        logger.info(f"Đang xử lý file: {filename}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                json_data = json.loads(content)
            
            is_valid, error_msg = self.validator.validate(json_data)
            if not is_valid:
                logger.error(f"Kiểm tra thất bại: {error_msg}")
                self.load_raw_json_failed(file_path, error_msg)
                if not keep_file:
                    self.move_file(file_path, config.LOAD_STATUS_FAILED)
                return False
            
            existing_video_ids = self.raw_json_manager.fetch_existing_video_ids()
            logger.info(f"Tìm thấy {len(existing_video_ids)} video ID đã tồn tại trong RawJson")
            
            new_items = []
            old_count = 0
            
            if isinstance(json_data, list):
                for item in json_data:
                    video_id = item.get('id')
                    if video_id and video_id not in existing_video_ids:
                        new_items.append(item)
                    else:
                        old_count += 1
            
            logger.info(f"Đã lọc: {len(new_items)} items MỚI, {old_count} items CŨ")
            
            if not new_items:
                logger.info("Không có items mới để xử lý. Bỏ qua file.")
                if not keep_file:
                    self.move_file(file_path, config.LOAD_STATUS_SUCCESS)
                return True
            
            if not self.load_raw_json(file_path, content):
                logger.error("Lưu raw JSON thất bại")
                if not keep_file:
                    self.move_file(file_path, config.LOAD_STATUS_FAILED)
                return False
            
            if skip_staging:
                logger.info("Đã bỏ qua xử lý staging")
                if not keep_file:
                    self.move_file(file_path, config.LOAD_STATUS_SUCCESS)
                return True
            
            result = TikTokTransformer.transform_batch(new_items)
            logger.info(f"Đã chuyển đổi {len(new_items)} items")
            
            results = self.process_staging_tables(
                authors=result['authors'],
                videos=result['videos'],
                interactions=result['interactions'],
                batch_id=BATCH_ID,
                source_filename=filename
            )
            
            logger.info(f"Đã xử lý file: {results}")
            
            if not keep_file:
                self.move_file(file_path, config.LOAD_STATUS_SUCCESS)
            
            return True
            
        except json.JSONDecodeError as e:
            error_msg = f"Lỗi giải mã JSON: {str(e)}"
            logger.error(error_msg)
            self.load_raw_json_failed(file_path, error_msg)
            if not keep_file:
                self.move_file(file_path, config.LOAD_STATUS_FAILED)
            return False
        except Exception as e:
            error_msg = f"Lỗi không mong đợi: {str(e)}"
            logger.error(error_msg)
            self.load_raw_json_failed(file_path, error_msg)
            if not keep_file:
                self.move_file(file_path, config.LOAD_STATUS_FAILED)
            return False
    
    def process_directory(self, skip_staging: bool = False, keep_files: bool = False) -> Dict[str, Any]:
        logger.info(f"Bắt đầu quét thư mục: {config.STORAGE_PATH}")
        
        stats = {
            "total_files": 0,
            "successful_files": 0,
            "failed_files": 0,
            "skipped_files": 0,
        }
        
        json_files = list(Path(config.STORAGE_PATH).glob(config.JSON_FILE_PATTERN))
        json_files = [f for f in json_files if f.is_file()]
        
        logger.info(f"Tìm thấy {len(json_files)} file JSON")
        
        for json_file in json_files:
            stats["total_files"] += 1
            if self.process_file(str(json_file), skip_staging=skip_staging, keep_file=keep_files):
                stats["successful_files"] += 1
            else:
                stats["failed_files"] += 1
        
        logger.info(f"Hoàn thành xử lý thư mục: {stats}")
        return stats
    
    
    
    def cleanup(self):
        """Cleanup and close connections"""
        logger.info("Đang dọn dẹp...")
        if self.db_conn.is_connected():
            self.db_conn.disconnect()

class LoaderScheduler:
    """Manages scheduled loader runs"""
    
    def __init__(self, loader: TikTokLoader):
        self.loader = loader
        self.scheduler = BackgroundScheduler(config.SCHEDULER_CONFIG)
    
    def start(self, cron_expression: str):
        trigger = CronTrigger.from_crontab(cron_expression, timezone="Asia/Ho_Chi_Minh")
        self.scheduler.add_job(
            self.loader.process_directory,
            trigger=trigger,
            id="loader_job",
            name="TikTok Loader Job",
            replace_existing=True
        )
        self.scheduler.start()
        logger.info(f"Đã khởi động scheduler với cron: {cron_expression}")
    
    def stop(self):
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Đã dừng scheduler")

# ============================================================================
# CLI Interface
# ============================================================================

def load_date_dim_simple(csv_path: str) -> bool:
    """Load DateDim using simple LOAD DATA INFILE method"""
    logger.info(f"Đang tải DateDim từ {csv_path} (chế độ nhanh)...")
    
    try:
        db_conn = DatabaseConnection()
        db_conn.connect()
        date_dim_manager = DateDimManager(db_conn)
        
        success = date_dim_manager.load_date_dim_from_csv(csv_path)
        
        db_conn.disconnect()
        return success
    except Exception as e:
        logger.error(f"Tải thất bại: {e}")
        return False

def load_date_dim_validated(csv_path: str, verbose: bool = False) -> bool:
    """Load DateDim with full validation and statistics"""
    logger.info(f"Đang tải DateDim từ {csv_path} (chế độ kiểm tra)...")
    
    try:
        db_conn = DatabaseConnection()
        db_conn.connect()
        date_dim_manager = DateDimManager(db_conn)
        
        success, stats = date_dim_manager.load_date_dim_with_validation(csv_path)
        
        # Print statistics
        print("\n" + "=" * 80)
        print("Thống Kê Tải DateDim")
        print("=" * 80)
        print(f"Trạng thái: {'✓ THÀNH CÔNG' if success else '✗ THẤT BẠI'}")
        print(f"Tổng số bản ghi: {stats['total_records']}")
        print(f"Đã tải: {stats['loaded_records']}")
        print(f"Bỏ qua: {stats['skipped_records']}")
        print(f"Thời gian: {stats['duration_seconds']:.2f}s")
        
        if stats['errors']:
            print(f"\nGặp lỗi: {len(stats['errors'])}")
            if verbose:
                for i, error in enumerate(stats['errors'][:20], 1):
                    print(f"  {i}. {error}")
                if len(stats['errors']) > 20:
                    print(f"  ... và {len(stats['errors']) - 20} lỗi khác")
            else:
                for i, error in enumerate(stats['errors'][:5], 1):
                    print(f"  {i}. {error}")
                if len(stats['errors']) > 5:
                    print(f"  ... và {len(stats['errors']) - 5} lỗi khác")
                print("\n  Dùng --verbose để xem tất cả lỗi")
        else:
            print("\n✓ Không có lỗi kiểm tra!")
        
        print("=" * 80 + "\n")
        
        db_conn.disconnect()
        return success
    except Exception as e:
        logger.error(f"Load failed: {e}")
        return False

def verify_date_dim() -> bool:
    """Verify DateDim table integrity"""
    logger.info("Đang kiểm tra DateDim...")
    
    try:
        db_conn = DatabaseConnection()
        db_conn.connect()
        
        with db_conn.get_cursor() as cursor:
            # Count records
            cursor.execute("SELECT COUNT(*) as cnt FROM DateDim")
            total = cursor.fetchone()[0]
            print(f"✓ Tổng số bản ghi trong database: {total}")
            
            # Get date range
            cursor.execute("SELECT MIN(full_date), MAX(full_date) FROM DateDim")
            min_date, max_date = cursor.fetchone()
            print(f"✓ Khoảng ngày: {min_date} đến {max_date}")
            
            # Sample records
            cursor.execute("SELECT COUNT(*) FROM DateDim WHERE day_type = 'Weekend'")
            weekend_count = cursor.fetchone()[0]
            print(f"✓ Ngày cuối tuần: {weekend_count}")
            
            cursor.execute("SELECT COUNT(*) FROM DateDim WHERE day_type = 'Weekday'")
            weekday_count = cursor.fetchone()[0]
            print(f"✓ Ngày trong tuần: {weekday_count}")
            
            # Check for duplicates
            cursor.execute(
                "SELECT COUNT(*) FROM (SELECT date_sk FROM DateDim GROUP BY date_sk HAVING COUNT(*) > 1) t"
            )
            duplicates = cursor.fetchone()[0]
            if duplicates > 0:
                print(f"✗ Tìm thấy {duplicates} giá trị date_sk trùng lặp!")
                return False
            else:
                print("✓ Không có giá trị date_sk trùng lặp")
        
        db_conn.disconnect()
        return True
    except Exception as e:
        logger.error(f"Kiểm tra thất bại: {e}")
        return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="TikTok Loader Staging Service",
        epilog="Examples:\n"
               "  python loader.py                           # Run full pipeline\n"
               "  python loader.py --load_raw                # Only load raw JSON\n"
               "  python loader.py --load_staging            # Only load staging tables\n"
               "  python loader.py --schedule                # Run with scheduler"
    )
    
    parser.add_argument("--load_raw", action="store_true", help="Only load raw JSON")
    parser.add_argument("--load_staging", action="store_true", help="Only load staging tables")
    parser.add_argument("--no-remove", action="store_true", help="Don't move files after processing")
    parser.add_argument("--schedule", action="store_true", help="Run with scheduler")
    parser.add_argument("--simple", action="store_true", help="Use simple LOAD DATA INFILE")
    parser.add_argument("--verbose", action="store_true", help="Show detailed errors")
    parser.add_argument("--csv", default=config.DATE_DIM_PATH, help=f"CSV path (default: {config.DATE_DIM_PATH})")
    
    args = parser.parse_args()
    
  
    
    
    
    logger.info("=" * 80)
    logger.info("Dịch Vụ TikTok Loader Staging Đã Khởi Động")
    logger.info("=" * 80)
    
    try:
        loader = TikTokLoader()
        
        skip_staging = args.load_raw
        keep_files = args.no_remove
        
        if args.schedule:
            scheduler = LoaderScheduler(loader)
            scheduler.start(config.LOADER_SCHEDULE_CRON)
            logger.info("Chế độ scheduler đang hoạt động. Nhấn Ctrl+C để dừng.")
            try:
                while True:
                    import time
                    time.sleep(1)
            except KeyboardInterrupt:
                scheduler.stop()
                logger.info("Người dùng đã dừng scheduler")
        else:
            # Single run
            stats = loader.process_directory(skip_staging=skip_staging, keep_files=keep_files)
            logger.info(f"Thống kê cuối cùng: {stats}")
        
        loader.cleanup()
        logger.info("Loader hoàn thành thành công")
        
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
