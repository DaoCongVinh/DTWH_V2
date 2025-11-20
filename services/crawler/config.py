# config.py
import os
import yaml
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Load config.yml
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config.yml")
with open(CONFIG_PATH, "r") as f:
    cfg = yaml.safe_load(f)

# Apify Config
APIFY_TOKEN = os.getenv("APIFY_TOKEN") or cfg.get("apify", {}).get("token")
APIFY_ACTOR = os.getenv("APIFY_ACTOR") or cfg.get("apify", {}).get("actor_id")

# App Config
DEVICE_ID = cfg.get("app", {}).get("device_id", "device-unknown")
STORAGE_PATH = cfg.get("app", {}).get("storage_path", "/data/storage")
FILE_PATTERN = cfg.get("app", {}).get("file_pattern", "*.json")
DATE_FORMAT = cfg.get("app", {}).get("date_format", "%Y-%m-%d")

# MySQL Config
MYSQL_CONF = cfg.get("mysql") or {}
MYSQL_HOST = os.getenv("MYSQL_HOST", MYSQL_CONF.get("host", "db"))
MYSQL_PORT = int(os.getenv("MYSQL_PORT", MYSQL_CONF.get("port", 3306)))
MYSQL_USER = os.getenv("MYSQL_USER", MYSQL_CONF.get("user"))
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", MYSQL_CONF.get("password"))
MYSQL_DB = os.getenv("MYSQL_DATABASE", MYSQL_CONF.get("database"))

# Schedule Config
SCHEDULE_CONF = cfg.get("schedule", {})
SCHEDULE_ENABLED = SCHEDULE_CONF.get("enabled", True)
SCHEDULE_CRON = SCHEDULE_CONF.get("cron")
SCHEDULE_TIMEZONE = SCHEDULE_CONF.get("timezone", "UTC")

# Source Config
SOURCE_NAME = cfg.get("source", {}).get("name", "Apify Crawl")
SOURCE_URL = cfg.get("source", {}).get("url", "")

# Ensure storage path exists
os.makedirs(STORAGE_PATH, exist_ok=True)

# Email Config
MAIL_SENDER = os.getenv("MAIL_SENDER")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
MAIL_RECEIVER = os.getenv("MAIL_RECEIVER")