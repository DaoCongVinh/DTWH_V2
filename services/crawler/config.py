import os
import yaml
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# 1. Thiết lập đường dẫn gốc
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Load biến môi trường từ file .env
load_dotenv(os.path.join(BASE_DIR, ".env"))

# --- HÀM GỬI MAIL KHẨN CẤP (Dùng riêng cho Config) ---
def _send_emergency_alert(error_message):
    """
    Hàm này hoạt động độc lập, lấy thông tin trực tiếp từ os.getenv
    để đảm bảo gửi được mail kể cả khi config chưa load xong.
    """
    sender = os.getenv("MAIL_SENDER")
    password = os.getenv("MAIL_PASSWORD")
    receiver = os.getenv("MAIL_RECEIVER")
    device_id = os.getenv("DEVICE_ID", "Unknown-Device")

    if not sender or not password or not receiver:
        print("❌ CRITICAL: Không thể gửi mail báo lỗi Config vì thiếu thông tin Email trong .env")
        return

    subject = f"[CRITICAL] Config Load Failed - {device_id}"
    body = f"""
    <h3 style="color: red;">Hệ thống gặp lỗi nghiêm trọng khi tải cấu hình!</h3>
    <p><b>Device:</b> {device_id}</p>
    <p><b>Error Detail:</b></p>
    <pre>{error_message}</pre>
    <p><i>Hệ thống sẽ cố gắng chạy tiếp bằng biến môi trường (nếu có).</i></p>
    """

    try:
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = receiver
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, receiver, msg.as_string())
        server.quit()
        print("✅ Đã gửi email cảnh báo lỗi Config.")
    except Exception as e:
        print(f"❌ Không thể gửi email cảnh báo: {e}")

# 3. Xác định đường dẫn file config.yml
DEFAULT_YAML_PATH = os.path.join(BASE_DIR, "config.yml")
CONFIG_PATH = os.getenv("CONFIG_PATH", DEFAULT_YAML_PATH)

cfg = {}
if os.path.exists(CONFIG_PATH):
    try:
        with open(CONFIG_PATH, "r") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as e:
        # ===> KÍCH HOẠT GỬI MAIL KHI LỖI <===
        err_msg = f"Lỗi đọc file YAML tại {CONFIG_PATH}.\nChi tiết: {str(e)}"
        print(f"Warning: {err_msg}")
        _send_emergency_alert(err_msg)
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

# --- App Config ---
DEVICE_ID = get_conf("DEVICE_ID", "app", "device_id", "device-unknown")

_storage_default = os.path.join(BASE_DIR, "storage")
STORAGE_PATH = get_conf("STORAGE_PATH", "app", "storage_path", _storage_default)

if not os.path.exists(STORAGE_PATH):
    try:
        os.makedirs(STORAGE_PATH, exist_ok=True)
    except Exception as e:
        print(f"Error creating storage path {STORAGE_PATH}: {e}")

FILE_PATTERN = get_conf("FILE_PATTERN", "app", "file_pattern", "*.json")
DATE_FORMAT = get_conf("DATE_FORMAT", "app", "date_format", "%Y-%m-%d")
SOURCE_NAME = "tiktok"
SOURCE_URL = "https://www.tiktok.com"

# --- Apify Config ---
APIFY_TOKEN = get_conf("APIFY_TOKEN", "apify", "token")
APIFY_ACTOR = get_conf("APIFY_ACTOR", "apify", "actor_id")

# --- MySQL Config ---
MYSQL_HOST = get_conf("MYSQL_HOST", "mysql", "host", "localhost")
MYSQL_PORT = int(get_conf("MYSQL_PORT", "mysql", "port", 3306))
MYSQL_USER = get_conf("MYSQL_USER", "mysql", "user", "root")
MYSQL_PASSWORD = get_conf("MYSQL_PASSWORD", "mysql", "password", "")
MYSQL_DB = get_conf("MYSQL_DATABASE", "mysql", "database", "metadata_tiktok")

# --- Schedule Config ---
SCHEDULE_CRON = get_conf("SCHEDULE_CRON", "schedule", "cron", "0 8 * * *")
_sched_enabled = get_conf("SCHEDULE_ENABLED", "schedule", "enabled", "True")
SCHEDULE_ENABLED = str(_sched_enabled).lower() in ("true", "1", "yes", "on")
SCHEDULE_TIMEZONE = get_conf("SCHEDULE_TIMEZONE", "schedule", "timezone", "Asia/Ho_Chi_Minh")

# --- Email Config ---
MAIL_SENDER = os.getenv("MAIL_SENDER")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
MAIL_RECEIVER = os.getenv("MAIL_RECEIVER")