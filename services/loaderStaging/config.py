import os
import yaml
from dotenv import load_dotenv

# 1. Thiết lập đường dẫn gốc
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Load biến môi trường từ file .env
load_dotenv(os.path.join(BASE_DIR, ".env"))

# 3. Xác định đường dẫn file config.yml
DEFAULT_YAML_PATH = os.path.join(BASE_DIR, "config.yml")
CONFIG_PATH = os.getenv("CONFIG_PATH", DEFAULT_YAML_PATH)

cfg = {}
if os.path.exists(CONFIG_PATH):
    try:
        with open(CONFIG_PATH, "r") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as e:
        print(f"Warning: Lỗi đọc file YAML tại {CONFIG_PATH}. Chi tiết: {str(e)}")
else:
    print(f"Warning: config.yml not found at {CONFIG_PATH}. Using environment variables only.")

# ========================================================
# HÀM HỖ TRỢ LẤY CONFIG
# ========================================================
def get_conf(env_key, yaml_section, yaml_key, default=None):
    val = os.getenv(env_key)
    if val is not None:
        return val
    if cfg and yaml_section in cfg:
        return cfg[yaml_section].get(yaml_key, default)
    return default

# ========================================================
# CÁC BIẾN CẤU HÌNH (CONSTANTS)
# ========================================================

# --- MySQL Config ---
MYSQL_HOST = get_conf("MYSQL_HOST", "mysql", "host", "db")
MYSQL_PORT = int(get_conf("MYSQL_PORT", "mysql", "port", 3306))
MYSQL_USER = get_conf("MYSQL_USER", "mysql", "user")
MYSQL_PASSWORD = get_conf("MYSQL_PASSWORD", "mysql", "password")
MYSQL_DB_STAGING = get_conf("MYSQL_DATABASE_STAGING", "mysql", "database_staging", "dbStaging")

# --- Storage Config ---
STORAGE_PATH = get_conf("STORAGE_PATH", "app", "storage_path", "/data/storage")
SQL_INIT_PATH = get_conf("SQL_INIT_PATH", "app", "sql_init_path", "./loader.sql")
DATE_DIM_PATH = get_conf("DATE_DIM_PATH", "app", "date_dim_path", "./date_dim.csv")

# --- Schedule Config ---
SCHEDULE_CRON = get_conf("SCHEDULE_CRON", "schedule", "cron", "30 14 * * *")
_sched_enabled = get_conf("SCHEDULE_ENABLED", "schedule", "enabled", "True")
SCHEDULE_ENABLED = str(_sched_enabled).lower() in ("true", "1", "yes", "on")
SCHEDULE_TIMEZONE = get_conf("SCHEDULE_TIMEZONE", "schedule", "timezone", "Asia/Ho_Chi_Minh")

