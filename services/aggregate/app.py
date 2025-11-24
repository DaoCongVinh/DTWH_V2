import os
import pymysql
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

# ---- Load environment ----
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "db")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "dwhtiktok")
SOURCE_DB = os.getenv("MYSQL_DATABASE", "dwh_tiktok")  # DB gốc chứa dim và fact
AGG_DB = "dbAgg"  # DB lưu aggregate

# ---- Logger setup ----
logger = logging.getLogger("aggregate")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(ch)


# ---- Connect helper ----
def get_db_conn(db_name=None):
    """Tạo kết nối tới database (mặc định là SOURCE_DB)"""
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=db_name or SOURCE_DB,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )


# ---- Log to Control_log ----
def log_to_db(status, message, error_message=None):
    """Ghi log kết quả aggregate vào bảng Control_log"""
    try:
        conn = get_db_conn(SOURCE_DB)
        with conn.cursor() as cur:
            # Tìm id_config cho aggregate job
            cur.execute("SELECT id_config FROM Config_log WHERE source_name = %s LIMIT 1", ("aggregate",))
            row = cur.fetchone()
            id_config = row["id_config"] if row else None

            if not id_config:
                logger.warning("Không tìm thấy id_config cho 'aggregate' trong Config_log. Bỏ qua ghi log.")
                return

            sql = """
                  INSERT INTO Control_log (id_config, file_name, status, extract_time, total_record, error_message)
                  VALUES (%s, %s, %s, %s, %s, %s) \
                  """
            cur.execute(sql, (
                id_config, "aggregate_job", status,
                datetime.now(timezone.utc), 0, error_message
            ))
            logger.info(f"📋 Đã ghi log trạng thái {status} vào Control_log.")
    except Exception as e:
        logger.error(f"Ghi log thất bại: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


# ---- Khởi tạo database aggregate ----
def init_aggregate_db():
    """Tạo database dbAgg nếu chưa có"""
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        autocommit=True
    )
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {AGG_DB};")
    conn.close()
    logger.info(f"Database '{AGG_DB}' đã được tạo (hoặc đã tồn tại).")


# ---- Aggregate job ----
def create_aggregate_tables():
    """Tạo bảng aggregate từ dữ liệu trong SOURCE_DB và lưu vào dbAgg"""
    source_conn = get_db_conn(SOURCE_DB)
    agg_conn = get_db_conn(AGG_DB)

    try:
        with agg_conn.cursor() as cur:

            # --- Aggregate by author ---
            logger.info("Tạo bảng agg_author_performance ...")
            cur.execute("DROP TABLE IF EXISTS agg_author_performance;")
            cur.execute(f"""
                CREATE TABLE agg_author_performance AS
                SELECT
                    da.authorID AS s_key,
                    da.authorID,
                    da.authorName,
                    SUM(fv.playCount) AS totalViews,
                    SUM(fv.diggCount) AS totalLikes,
                    SUM(fv.commentCount) AS totalComments,
                    SUM(fv.shareCount) AS totalShares,
                    COUNT(fv.videoID) AS totalVideos,
                    ROUND(SUM(fv.playCount) / COUNT(fv.videoID), 2) AS avgViewsPerVideo
                FROM {SOURCE_DB}.fact_videos fv
                JOIN {SOURCE_DB}.dim_authors da ON fv.authorID = da.authorID
                GROUP BY da.authorID, da.authorName;
            """)

            # --- Aggregate by date ---
            logger.info("Tạo bảng agg_daily_performance ...")
            cur.execute("DROP TABLE IF EXISTS agg_daily_performance;")
            cur.execute(f"""
                CREATE TABLE agg_daily_performance AS
                SELECT
                    dd.dateKey AS s_key,
                    dd.dateKey,
                    dd.date AS fullDate,
                    dd.day AS dayName,
                    SUM(fv.playCount) AS totalViews,
                    SUM(fv.diggCount) AS totalLikes,
                    SUM(fv.commentCount) AS totalComments,
                    SUM(fv.shareCount) AS totalShares,
                    COUNT(fv.videoID) AS totalVideos
                FROM {SOURCE_DB}.fact_videos fv
                JOIN {SOURCE_DB}.dim_date dd ON fv.dateKey = dd.dateKey
                GROUP BY dd.dateKey, dd.date, dd.day
                ORDER BY dd.date;
            """)

            logger.info("Hai bảng aggregate đã được tạo thành công trong database dbAgg.")
            log_to_db("SUCCESS", "Aggregate tables created")

    except Exception as e:
        logger.exception("Lỗi khi tạo aggregate: %s", e)
        log_to_db("FAILED", "Aggregate failed", error_message=str(e))
    finally:
        source_conn.close()
        agg_conn.close()


# ---- Main entry ----
if __name__ == "__main__":
    logger.info("Bắt đầu quá trình tạo aggregate database...")
    init_aggregate_db()
    create_aggregate_tables()
    logger.info("Hoàn tất quá trình aggregate.")
