import pymysql
import config
from logging_setup import logger

def get_db_conn():
    """
    Kết nối Database. Nếu sai pass/host sẽ tự văng lỗi Exception.
    """
    return pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        db="metadata_tiktok",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10 # Thêm timeout để không treo quá lâu nếu mạng lag
    )

def save_config_to_db():
    """
    Lưu config. KHÔNG dùng try-except ở đây để lỗi được truyền ra ngoài.
    """
    conn = get_db_conn() # Nếu lỗi kết nối, nó dừng ngay tại đây -> nhảy về main_job -> gửi mail
    try:
        with conn.cursor() as cur:
            check_sql = "SELECT id_config FROM config_log WHERE source_name=%s AND source_url=%s"
            cur.execute(check_sql, (config.SOURCE_NAME, config.SOURCE_URL))
            row = cur.fetchone()
            if row:
                return row["id_config"]

            insert_sql = """
                INSERT INTO config_log (source_name, source_url, api_endpoint, file_path, file_pattern, date_format, schedule_time, is_active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cur.execute(insert_sql, (
                config.SOURCE_NAME, config.SOURCE_URL, config.APIFY_ACTOR, 
                config.STORAGE_PATH, config.FILE_PATTERN, config.DATE_FORMAT, 
                config.SCHEDULE_CRON, True
            ))
            return cur.lastrowid
    finally:
        conn.close()

def log_to_db(status, message, total_record=0, error_message=None, id_config=None):
    """
    Ghi log. Hàm này giữ try-except để nếu ghi log thất bại thì không làm crash luồng gửi mail.
    """
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            sql = """
                INSERT INTO control_log (id_config, file_name, status, extract_time, total_record, error_message)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql, (id_config, message, status, datetime.now(), total_record, error_message))
        conn.close()
    except Exception as e:
        logger.error("⚠️ Failed to write to DB control_log: %s", e)