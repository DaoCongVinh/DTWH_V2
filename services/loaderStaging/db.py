"""
Database Helper Module
Handles all database operations: connections, batch fetch, upsert, logging
"""

import logging
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
import mysql.connector
from mysql.connector import Error, MySQLConnection
from contextlib import contextmanager

import config

logger = logging.getLogger(__name__)

# ============================================================================
# Database Connection Manager
# ============================================================================

class DatabaseConnection:
    """Manages MySQL database connections"""
    
    def __init__(self):
        """Initialize database connection"""
        self.connection: Optional[MySQLConnection] = None
    
    def connect(self) -> MySQLConnection:
        """
        Establish database connection
        
        Returns:
            MySQLConnection: Active database connection
            
        Raises:
            Error: If connection fails
        """
        try:
            self.connection = mysql.connector.connect(
                host=config.MYSQL_HOST,
                port=config.MYSQL_PORT,
                user=config.MYSQL_USER,
                password=config.MYSQL_PASSWORD,
                database=config.MYSQL_DATABASE,
                charset=config.DB_CONFIG["charset"],
                autocommit=config.DB_CONFIG["autocommit"]
            )
            logger.info(f"Connected to database: {config.MYSQL_DATABASE}")
            return self.connection
        except Error as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")
    
    def is_connected(self) -> bool:
        """Check if connection is active"""
        return self.connection and self.connection.is_connected()
    
    @contextmanager
    def get_cursor(self, buffered: bool = False):
        """
        Context manager for database cursor
        
        Args:
            buffered: Use buffered cursor for multiple queries
            
        Yields:
            CMySQLCursor: Database cursor
        """
        if not self.is_connected():
            self.connect()
        
        cursor = self.connection.cursor(buffered=buffered)
        try:
            yield cursor
        finally:
            cursor.close()

# ============================================================================
# Batch Fetch Operations
# ============================================================================

class BatchFetcher:
    """Batch fetch operations to optimize ETL"""
    
    def __init__(self, db_conn: DatabaseConnection):
        """
        Initialize batch fetcher
        
        Args:
            db_conn: Database connection instance
        """
        self.db_conn = db_conn
    
    def fetch_all_authors(self) -> Set[str]:
        """
        Fetch all existing author IDs
        
        Returns:
            Set[str]: Set of all existing author IDs
        """
        try:
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(config.Queries.GET_ALL_AUTHORS)
                results = cursor.fetchall()
                author_ids = {row[0] for row in results}
                logger.info(f"Fetched {len(author_ids)} existing authors")
                return author_ids
        except Error as e:
            logger.error(f"Error fetching authors: {e}")
            return set()
    
    def fetch_all_videos(self) -> Set[str]:
        """
        Fetch all existing video IDs
        
        Returns:
            Set[str]: Set of all existing video IDs
        """
        try:
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(config.Queries.GET_ALL_VIDEOS)
                results = cursor.fetchall()
                video_ids = {row[0] for row in results}
                logger.info(f"Fetched {len(video_ids)} existing videos")
                return video_ids
        except Error as e:
            logger.error(f"Error fetching videos: {e}")
            return set()
    
    def fetch_all_interactions(self) -> Set[str]:
        """
        Fetch all video IDs with existing interactions
        
        Returns:
            Set[str]: Set of video IDs with interactions
        """
        try:
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(config.Queries.GET_ALL_INTERACTIONS)
                results = cursor.fetchall()
                video_ids = {row[0] for row in results}
                logger.info(f"Fetched {len(video_ids)} videos with interactions")
                return video_ids
        except Error as e:
            logger.error(f"Error fetching interactions: {e}")
            return set()
    
    def fetch_all(self) -> Tuple[Set[str], Set[str], Set[str]]:
        """
        Batch fetch all data in 3 queries
        
        Returns:
            Tuple containing:
                - Set of author IDs
                - Set of video IDs
                - Set of video IDs with interactions
        """
        logger.info("Starting batch fetch...")
        authors = self.fetch_all_authors()
        videos = self.fetch_all_videos()
        interactions = self.fetch_all_interactions()
        logger.info(f"Batch fetch complete: {len(authors)} authors, {len(videos)} videos, {len(interactions)} interactions")
        return authors, videos, interactions
    
    def get_today_date_sk(self) -> Optional[int]:
        """
        Get today's date surrogate key
        
        Returns:
            Optional[int]: Today's date_sk or None if not found
        """
        try:
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(config.Queries.GET_TODAY_DATE_SK)
                result = cursor.fetchone()
                if result:
                    date_sk = result[0]
                    logger.info(f"Today's date_sk: {date_sk}")
                    return date_sk
                logger.warning("Today's date not found in DateDim")
                return None
        except Error as e:
            logger.error(f"Error getting today's date_sk: {e}")
            return None

# ============================================================================
# Raw JSON Operations
# ============================================================================

class RawJsonManager:
    """Manage RawJson table operations"""
    
    def __init__(self, db_conn: DatabaseConnection):
        """
        Initialize raw json manager
        
        Args:
            db_conn: Database connection instance
        """
        self.db_conn = db_conn
    
    def insert_raw_json(
        self,
        content: str,
        filename: str,
        status: str = config.LOAD_STATUS_SUCCESS,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Insert raw JSON record
        
        Args:
            content: Full JSON content
            filename: Source file name
            status: Load status (SUCCESS/FAILED)
            error_message: Error details if failed
            
        Returns:
            bool: True if successful
        """
        try:
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(
                    config.Queries.INSERT_RAW_JSON,
                    (content, filename, status, error_message)
                )
                self.db_conn.connection.commit()
                logger.info(f"Inserted raw JSON record: {filename} ({status})")
                return True
        except Error as e:
            logger.error(f"Error inserting raw JSON: {e}")
            self.db_conn.connection.rollback()
            return False

# ============================================================================
# Upsert Operations (SCD Type 2)
# ============================================================================

class UpsertManager:
    """Manage upsert operations with SCD Type 2 logic"""
    
    def __init__(self, db_conn: DatabaseConnection):
        """
        Initialize upsert manager
        
        Args:
            db_conn: Database connection instance
        """
        self.db_conn = db_conn
    
    def upsert_author(
        self,
        author_id: str,
        author_name: str,
        avatar: str,
        date_sk: int,
        existing_authors: Set[str],
        existing_data: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:  # (success, action: INSERT/UPDATE/SKIP)
        """
        Upsert author record (SCD Type 2)
        
        Args:
            author_id: Author ID
            author_name: Author name
            avatar: Avatar URL
            date_sk: Date surrogate key
            existing_authors: Set of existing author IDs
            existing_data: Existing author data for comparison
            
        Returns:
            Tuple of (success, action_taken)
        """
        try:
            if author_id not in existing_authors:
                # INSERT new author
                with self.db_conn.get_cursor() as cursor:
                    cursor.execute(
                        config.Queries.INSERT_AUTHOR,
                        (author_id, author_name, avatar, date_sk)
                    )
                self.db_conn.connection.commit()
                return True, "INSERT"
            else:
                # Author exists - check if data changed
                if existing_data and (
                    existing_data.get("author_name") == author_name and
                    existing_data.get("avatar") == avatar
                ):
                    # No change - SKIP
                    return True, "SKIP"
                else:
                    # Data changed - UPDATE
                    with self.db_conn.get_cursor() as cursor:
                        cursor.execute(
                            config.Queries.UPDATE_AUTHOR,
                            (author_name, avatar, author_id, date_sk)
                        )
                    self.db_conn.connection.commit()
                    return True, "UPDATE"
        except Error as e:
            logger.error(f"Error upserting author {author_id}: {e}")
            self.db_conn.connection.rollback()
            return False, "ERROR"
    
    def upsert_video(
        self,
        video_id: str,
        author_id: str,
        text_content: str,
        duration: int,
        create_time: str,
        web_video_url: str,
        date_sk: int,
        existing_videos: Set[str],
        existing_data: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """
        Upsert video record (SCD Type 2)
        
        Args:
            video_id: Video ID
            author_id: Author ID (FK)
            text_content: Video caption
            duration: Duration in seconds
            create_time: Creation timestamp
            web_video_url: TikTok URL
            date_sk: Date surrogate key
            existing_videos: Set of existing video IDs
            existing_data: Existing video data for comparison
            
        Returns:
            Tuple of (success, action_taken)
        """
        try:
            if video_id not in existing_videos:
                # INSERT new video
                with self.db_conn.get_cursor() as cursor:
                    cursor.execute(
                        config.Queries.INSERT_VIDEO,
                        (video_id, author_id, text_content, duration, create_time, web_video_url, date_sk)
                    )
                self.db_conn.connection.commit()
                return True, "INSERT"
            else:
                # Video exists - check if data changed
                if existing_data and (
                    existing_data.get("text_content") == text_content and
                    existing_data.get("duration") == duration and
                    existing_data.get("create_time") == create_time
                ):
                    # No change - SKIP
                    return True, "SKIP"
                else:
                    # Data changed - UPDATE
                    with self.db_conn.get_cursor() as cursor:
                        cursor.execute(
                            config.Queries.UPDATE_VIDEO,
                            (text_content, duration, create_time, web_video_url, video_id, date_sk)
                        )
                    self.db_conn.connection.commit()
                    return True, "UPDATE"
        except Error as e:
            logger.error(f"Error upserting video {video_id}: {e}")
            self.db_conn.connection.rollback()
            return False, "ERROR"
    
    def upsert_interaction(
        self,
        video_id: str,
        digg_count: int,
        play_count: int,
        share_count: int,
        comment_count: int,
        collect_count: int,
        date_sk: int,
        existing_interactions: Set[str],
        existing_data: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """
        Upsert video interaction record (SCD Type 2)
        
        Args:
            video_id: Video ID
            digg_count: Like count
            play_count: View count
            share_count: Share count
            comment_count: Comment count
            collect_count: Save count
            date_sk: Date surrogate key
            existing_interactions: Set of video IDs with interactions
            existing_data: Existing interaction data for comparison
            
        Returns:
            Tuple of (success, action_taken)
        """
        try:
            if video_id not in existing_interactions:
                # INSERT new interaction
                with self.db_conn.get_cursor() as cursor:
                    cursor.execute(
                        config.Queries.INSERT_INTERACTION,
                        (video_id, digg_count, play_count, share_count, comment_count, collect_count, date_sk)
                    )
                self.db_conn.connection.commit()
                return True, "INSERT"
            else:
                # Interaction exists - check if counts changed
                if existing_data and (
                    existing_data.get("digg_count") == digg_count and
                    existing_data.get("play_count") == play_count and
                    existing_data.get("share_count") == share_count and
                    existing_data.get("comment_count") == comment_count and
                    existing_data.get("collect_count") == collect_count
                ):
                    # No change - SKIP
                    return True, "SKIP"
                else:
                    # Data changed - UPDATE
                    with self.db_conn.get_cursor() as cursor:
                        cursor.execute(
                            config.Queries.UPDATE_INTERACTION,
                            (digg_count, play_count, share_count, comment_count, collect_count, video_id, date_sk)
                        )
                    self.db_conn.connection.commit()
                    return True, "UPDATE"
        except Error as e:
            logger.error(f"Error upserting interaction {video_id}: {e}")
            self.db_conn.connection.rollback()
            return False, "ERROR"

# ============================================================================
# Load Log Manager
# ============================================================================

class LoadLogManager:
    """Manage LoadLog table operations"""
    
    def __init__(self, db_conn: DatabaseConnection):
        """
        Initialize load log manager
        
        Args:
            db_conn: Database connection instance
        """
        self.db_conn = db_conn
    
    def log_load(
        self,
        batch_id: str,
        table_name: str,
        record_count: int,
        inserted_count: int,
        updated_count: int,
        skipped_count: int,
        status: str,
        start_time: datetime,
        end_time: datetime,
        source_filename: str,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Insert load log record
        
        Args:
            batch_id: Batch identifier
            table_name: Target table name
            record_count: Total records processed
            inserted_count: Records inserted
            updated_count: Records updated
            skipped_count: Records skipped
            status: Load status (SUCCESS/FAILED/PARTIAL)
            start_time: Start timestamp
            end_time: End timestamp
            source_filename: Source file name
            error_message: Error details if any
            
        Returns:
            bool: True if successful
        """
        try:
            duration_seconds = (end_time - start_time).total_seconds()
            
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(
                    config.Queries.INSERT_LOAD_LOG,
                    (
                        batch_id, table_name, record_count,
                        inserted_count, updated_count, skipped_count,
                        status, start_time, end_time,
                        duration_seconds, source_filename, error_message
                    )
                )
            self.db_conn.connection.commit()
            logger.info(
                f"Logged: {table_name} - {inserted_count} inserted, {updated_count} updated, "
                f"{skipped_count} skipped in {duration_seconds:.2f}s"
            )
            return True
        except Error as e:
            logger.error(f"Error logging load: {e}")
            self.db_conn.connection.rollback()
            return False

# ============================================================================
# DateDim Manager
# ============================================================================

class DateDimManager:
    """Manage DateDim table operations"""
    
    def __init__(self, db_conn: DatabaseConnection):
        """
        Initialize date dim manager
        
        Args:
            db_conn: Database connection instance
        """
        self.db_conn = db_conn
    
    def load_date_dim_from_csv(self, csv_path: str) -> bool:
        """
        Load DateDim from CSV file (18 columns from date_dim.csv)
        
        CSV Columns:
        1. date_sk - Date surrogate key
        2. full_date - Full date (YYYY-MM-DD)
        3. day_since_2005 - Days since 2005-01-01
        4. month_since_2005 - Months since 2005-01-01
        5. day_of_week - Day name (Monday, Tuesday, etc.)
        6. calendar_month - Month name (January, February, etc.)
        7. calendar_year - Year (YYYY)
        8. calendar_year_month - Year-Month (YYYY-Mon)
        9. day_of_month - Day of month (1-31)
        10. day_of_year - Day of year (1-366)
        11. week_of_year_sunday - Week number (Sunday based)
        12. year_week_sunday - Year-Week (YYYY-Www) Sunday based
        13. week_sunday_start - Start date of week (Sunday)
        14. week_of_year_monday - Week number (Monday based)
        15. year_week_monday - Year-Week (YYYY-Www) Monday based
        16. week_monday_start - Start date of week (Monday)
        17. holiday - Holiday designation
        18. day_type - Weekend/Weekday
        
        Args:
            csv_path: Path to date_dim.csv file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            from pathlib import Path
            
            # Verify CSV file exists
            csv_file = Path(csv_path)
            if not csv_file.exists():
                logger.error(f"CSV file not found: {csv_path}")
                return False
            
            logger.info(f"Starting DateDim load from: {csv_path}")
            
            with self.db_conn.get_cursor() as cursor:
                # Clear existing data (optional, comment out if you want to preserve)
                clear_sql = f"TRUNCATE TABLE {config.Tables.DATE_DIM}"
                cursor.execute(clear_sql)
                logger.info(f"Cleared existing data from {config.Tables.DATE_DIM}")
                
                # Build column mapping for all 18 columns
                columns = (
                    "date_sk, full_date, day_since_2005, month_since_2005, "
                    "day_of_week, calendar_month, calendar_year, calendar_year_month, "
                    "day_of_month, day_of_year, week_of_year_sunday, year_week_sunday, "
                    "week_sunday_start, week_of_year_monday, year_week_monday, "
                    "week_monday_start, holiday, day_type"
                )
                
                # MySQL LOAD DATA INFILE command
                load_sql = f"""
                LOAD DATA LOCAL INFILE '{csv_path}'
                INTO TABLE {config.Tables.DATE_DIM}
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY '\\n'
                ({columns})
                """
                
                cursor.execute(load_sql)
            
            self.db_conn.connection.commit()
            
            # Verify load success
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as cnt FROM {config.Tables.DATE_DIM}")
                result = cursor.fetchone()
                row_count = result[0] if result else 0
                logger.info(f"Successfully loaded {row_count} rows into DateDim")
                
                if row_count == 0:
                    logger.warning("DateDim table is empty after load")
                    return False
            
            return True
            
        except Error as e:
            logger.error(f"Database error loading DateDim: {e}")
            if self.db_conn.connection:
                self.db_conn.connection.rollback()
            return False
        except Exception as e:
            logger.error(f"Unexpected error loading DateDim: {e}")
            if self.db_conn.connection:
                self.db_conn.connection.rollback()
            return False
    
    def load_date_dim_with_validation(self, csv_path: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Load DateDim from CSV with detailed validation and error handling
        
        Args:
            csv_path: Path to date_dim.csv file
            
        Returns:
            Tuple of (success: bool, stats: Dict with load statistics)
        """
        stats = {
            "total_records": 0,
            "loaded_records": 0,
            "skipped_records": 0,
            "errors": [],
            "start_time": datetime.now(),
            "end_time": None,
            "duration_seconds": 0
        }
        
        try:
            from pathlib import Path
            import csv
            
            # Verify CSV file exists
            csv_file = Path(csv_path)
            if not csv_file.exists():
                error_msg = f"CSV file not found: {csv_path}"
                logger.error(error_msg)
                stats["errors"].append(error_msg)
                return False, stats
            
            logger.info(f"Starting DateDim validation load from: {csv_path}")
            
            # Read and validate CSV data
            valid_records = []
            with open(csv_path, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                
                for line_num, row in enumerate(csv_reader, 1):
                    stats["total_records"] = line_num
                    
                    # Validate row has at least 18 columns (may have more, we only use first 18)
                    if len(row) < 18:
                        error_msg = f"Line {line_num}: Expected at least 18 columns, got {len(row)}"
                        logger.warning(error_msg)
                        stats["errors"].append(error_msg)
                        stats["skipped_records"] += 1
                        continue
                    
                    # Use only first 18 columns
                    row = row[:18]
                    
                    # Validate date_sk is numeric
                    try:
                        date_sk = int(row[0])
                    except ValueError:
                        error_msg = f"Line {line_num}: Invalid date_sk '{row[0]}' (must be numeric)"
                        logger.warning(error_msg)
                        stats["errors"].append(error_msg)
                        stats["skipped_records"] += 1
                        continue
                    
                    # Validate full_date format
                    if len(row[1]) != 10 or row[1].count('-') != 2:
                        error_msg = f"Line {line_num}: Invalid date format '{row[1]}' (expected YYYY-MM-DD)"
                        logger.warning(error_msg)
                        stats["errors"].append(error_msg)
                        stats["skipped_records"] += 1
                        continue
                    
                    valid_records.append(row)
            
            if not valid_records:
                error_msg = "No valid records found in CSV file"
                logger.error(error_msg)
                stats["errors"].append(error_msg)
                return False, stats
            
            # Load valid records into database
            with self.db_conn.get_cursor() as cursor:
                # Disable foreign key checks temporarily
                cursor.execute("SET FOREIGN_KEY_CHECKS=0")
                
                # Clear existing data
                cursor.execute(f"TRUNCATE TABLE {config.Tables.DATE_DIM}")
                logger.info(f"Cleared existing data from {config.Tables.DATE_DIM}")
                
                # Prepare insert SQL
                insert_sql = f"""
                INSERT INTO {config.Tables.DATE_DIM} (
                    date_sk, full_date, day_since_2005, month_since_2005,
                    day_of_week, calendar_month, calendar_year, calendar_year_month,
                    day_of_month, day_of_year, week_of_year_sunday, year_week_sunday,
                    week_sunday_start, week_of_year_monday, year_week_monday,
                    week_monday_start, holiday, day_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                # Batch insert
                cursor.executemany(insert_sql, valid_records)
                
                # Re-enable foreign key checks
                cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            
            self.db_conn.connection.commit()
            stats["loaded_records"] = len(valid_records)
            
            logger.info(f"Successfully loaded {stats['loaded_records']} records into DateDim")
            
            stats["end_time"] = datetime.now()
            stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
            
            return True, stats
            
        except Error as e:
            error_msg = f"Database error loading DateDim: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
            if self.db_conn.connection:
                self.db_conn.connection.rollback()
            stats["end_time"] = datetime.now()
            stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
            return False, stats
        except Exception as e:
            error_msg = f"Unexpected error loading DateDim: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
            if self.db_conn.connection:
                self.db_conn.connection.rollback()
            stats["end_time"] = datetime.now()
            stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
            return False, stats
