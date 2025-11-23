"""
Configuration file for Loader Staging Service
Manages environment variables and application constants
"""

import os
from pathlib import Path
from typing import Optional

# ============================================================================
# Environment Variables (with defaults)
# ============================================================================

# Database Configuration
MYSQL_HOST: str = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT: int = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER: str = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD: str = os.getenv("MYSQL_PASSWORD", "rootpass")
MYSQL_DATABASE: str = os.getenv("MYSQL_DATABASE", "dbStaging")

# Storage Configuration
STORAGE_PATH: str = os.getenv("STORAGE_PATH", "/data/storage")
DATE_DIM_PATH: str = os.getenv("DATE_DIM_PATH", "./date_dim.csv")

# Loader Scheduler Configuration
LOADER_SCHEDULE_ENABLED: bool = os.getenv("SCHEDULE_ENABLED", "False").lower() == "true"
LOADER_SCHEDULE_CRON: str = os.getenv("SCHEDULE_CRON", "0 */1 * * *")

# Application Configuration
APP_ENV: str = os.getenv("APP_ENV", "development")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
DEBUG_MODE: bool = os.getenv("DEBUG_MODE", "False").lower() == "true"

# ============================================================================
# Constants
# ============================================================================

# File paths
FAILED_DIR: str = os.path.join(STORAGE_PATH, "failed")
PROCESSED_DIR: str = os.path.join(STORAGE_PATH, "processed")
SCHEMA_FILE: str = os.path.join(os.path.dirname(__file__), "tiktok_schema.json")

# Database constants
DB_CONFIG = {
    "host": MYSQL_HOST,
    "port": MYSQL_PORT,
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "database": MYSQL_DATABASE,
    "charset": "utf8mb4",
    "autocommit": False,
}

# Load Status
LOAD_STATUS_SUCCESS = "SUCCESS"
LOAD_STATUS_FAILED = "FAILED"
LOAD_STATUS_PARTIAL = "PARTIAL"

# Batch ID format
BATCH_ID_FORMAT = "LOAD_{timestamp}"

# Maximum items per batch
MAX_ITEMS_PER_BATCH = int(os.getenv("MAX_ITEMS_PER_BATCH", "1000"))

# File patterns
JSON_FILE_PATTERN = "*.json"
CSV_FILE_PATTERN = "*.csv"

# ============================================================================
# Loader Modes
# ============================================================================
class LoaderMode:
    """Loader execution modes"""
    FULL = "full"                    # Full pipeline
    RAW_ONLY = "raw"                 # Only load raw JSON
    STAGING_ONLY = "staging"         # Only load staging tables
    NO_REMOVE = "no_remove"          # Don't move/remove files
    SCHEDULE = "schedule"            # Run with scheduler

# ============================================================================
# Database Tables
# ============================================================================
class Tables:
    """Database table names"""
    RAW_JSON = "RawJson"
    LOAD_LOG = "LoadLog"
    DATE_DIM = "DateDim"
    AUTHORS = "Authors"
    VIDEOS = "Videos"
    VIDEO_INTERACTIONS = "VideoInteractions"

# ============================================================================
# SQL Queries
# ============================================================================
class Queries:
    """Common SQL queries"""
    
    GET_TODAY_DATE_SK = """
    SELECT date_sk FROM DateDim WHERE full_date = CURDATE()
    """
    
    GET_ALL_AUTHORS = """
    SELECT DISTINCT author_id FROM Authors
    """
    
    GET_ALL_VIDEOS = """
    SELECT DISTINCT video_id FROM Videos
    """
    
    GET_ALL_INTERACTIONS = """
    SELECT DISTINCT video_id FROM VideoInteractions
    """
    
    INSERT_RAW_JSON = """
    INSERT INTO RawJson (content, filename, load_status, loaded_at, error_message)
    VALUES (%s, %s, %s, NOW(), %s)
    """
    
    INSERT_AUTHOR = """
    INSERT INTO Authors (author_id, author_name, avatar, extract_date_sk, is_current)
    VALUES (%s, %s, %s, %s, TRUE)
    """
    
    UPDATE_AUTHOR = """
    UPDATE Authors 
    SET author_name = %s, avatar = %s, updated_at = NOW()
    WHERE author_id = %s AND extract_date_sk = %s
    """
    
    INSERT_VIDEO = """
    INSERT INTO Videos 
    (video_id, author_id, text_content, duration, create_time, web_video_url, create_date_sk, is_current)
    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
    """
    
    UPDATE_VIDEO = """
    UPDATE Videos 
    SET text_content = %s, duration = %s, create_time = %s, web_video_url = %s, updated_at = NOW()
    WHERE video_id = %s AND create_date_sk = %s
    """
    
    INSERT_INTERACTION = """
    INSERT INTO VideoInteractions 
    (video_id, digg_count, play_count, share_count, comment_count, collect_count, interaction_date_sk, is_current)
    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
    """
    
    UPDATE_INTERACTION = """
    UPDATE VideoInteractions 
    SET digg_count = %s, play_count = %s, share_count = %s, comment_count = %s, collect_count = %s, updated_at = NOW()
    WHERE video_id = %s AND interaction_date_sk = %s
    """
    
    INSERT_LOAD_LOG = """
    INSERT INTO LoadLog 
    (batch_id, table_name, record_count, inserted_count, updated_count, skipped_count, 
     status, start_time, end_time, duration_seconds, source_filename, error_message)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

# ============================================================================
# Validation Constants
# ============================================================================
class Validation:
    """Validation constants"""
    MIN_AUTHORS_PER_FILE = 1
    MAX_AUTHORS_PER_FILE = 10000
    
    MIN_VIDEOS_PER_FILE = 1
    MAX_VIDEOS_PER_FILE = 10000
    
    MIN_INTERACTIONS_PER_VIDEO = 0
    
    # Field length limits
    MAX_AUTHOR_ID_LENGTH = 50
    MAX_VIDEO_ID_LENGTH = 50
    MAX_AUTHOR_NAME_LENGTH = 255
    MAX_TEXT_CONTENT_LENGTH = 65535  # TEXT type max
    MAX_URL_LENGTH = 1024
    
    # Field value ranges
    MAX_DURATION = 600  # 10 minutes in seconds
    MAX_STAT_COUNT = 9999999999  # Max reasonable count

# ============================================================================
# Logging Configuration
# ============================================================================
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = os.getenv("LOG_FILE", "loader.log")

# ============================================================================
# Scheduler Configuration
# ============================================================================
SCHEDULER_CONFIG = {
    "apscheduler.jobstores.default": {
        "type": "memory"
    },
    "apscheduler.executors.default": {
        "type": "threadpool",
        "max_workers": 1
    },
    "apscheduler.job_defaults.coalesce": True,
    "apscheduler.job_defaults.max_instances": 1,
    "apscheduler.timezone": "Asia/Ho_Chi_Minh"
}

# ============================================================================
# Helper Functions
# ============================================================================

def ensure_directories() -> None:
    """Create required directories if they don't exist"""
    Path(STORAGE_PATH).mkdir(parents=True, exist_ok=True)
    Path(FAILED_DIR).mkdir(parents=True, exist_ok=True)
    Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)

def get_db_connection_string() -> str:
    """Generate MySQL connection string"""
    return f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

def validate_config() -> None:
    """Validate critical configuration"""
    assert MYSQL_HOST, "MYSQL_HOST is required"
    assert MYSQL_USER, "MYSQL_USER is required"
    assert MYSQL_PASSWORD, "MYSQL_PASSWORD is required"
    assert STORAGE_PATH, "STORAGE_PATH is required"
    assert Path(DATE_DIM_PATH).exists(), f"DATE_DIM_PATH not found: {DATE_DIM_PATH}"
    
    ensure_directories()

# ============================================================================
# Initialize on module load
# ============================================================================

if __name__ != "__main__":
    # Only validate in production
    if APP_ENV == "production":
        try:
            validate_config()
        except AssertionError as e:
            print(f"Configuration validation error: {e}")
            raise
