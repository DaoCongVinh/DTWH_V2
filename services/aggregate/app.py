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

# DB nguồn mới của bạn
SOURCE_DB = "warehouse_tiktok"
AGG_DB = "dbAgg"

# ---- Logger setup ----
logger = logging.getLogger("aggregate")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(ch)


# ---- Connect helper ----
def get_db_conn(db_name=None):
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=db_name or SOURCE_DB,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )


# ---- Khởi tạo database aggregate ----
def init_aggregate_db():
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
    logger.info(f"Database '{AGG_DB}' đã tồn tại hoặc đã được tạo mới.")


# ---- Aggregate job ----
def create_aggregate_tables():

    source_conn = get_db_conn(SOURCE_DB)
    agg_conn = get_db_conn(AGG_DB)

    try:
        with agg_conn.cursor() as cur:

            # ===================================================================
            # 1) Aggregate theo AUTHOR
            # ===================================================================
            logger.info("Đang tạo bảng agg_author_performance ...")

            cur.execute("DROP TABLE IF EXISTS agg_author_performance;")
            cur.execute(f"""
                CREATE TABLE agg_author_performance AS
                SELECT
                    da.author_id          AS s_key,          -- natural key
                    da.author_sk          AS author_sk,      -- surrogate key
                    da.author_name,
                    SUM(f.play_count)     AS totalViews,
                    SUM(f.digg_count)     AS totalLikes,
                    SUM(f.comment_count)  AS totalComments,
                    SUM(f.share_count)    AS totalShares,
                    COUNT(f.video_id)     AS totalVideos,
                    ROUND(SUM(f.play_count) / COUNT(f.video_id), 2) AS avgViewsPerVideo
                FROM {SOURCE_DB}.fact_video_interactions f
                JOIN {SOURCE_DB}.dim_authors da
                        ON f.author_sk = da.author_sk
                GROUP BY da.author_id, da.author_sk, da.author_name;
            """)

            # ===================================================================
            # 2) Aggregate theo DATE
            # ===================================================================
            logger.info("Đang tạo bảng agg_daily_performance ...")

            cur.execute("DROP TABLE IF EXISTS agg_daily_performance;")
            cur.execute(f"""
                CREATE TABLE agg_daily_performance AS
                SELECT
                    d.date_sk        AS s_key,
                    d.full_date      AS fullDate,
                    d.day_of_week    AS dayName,
                    SUM(f.play_count)    AS totalViews,
                    SUM(f.digg_count)    AS totalLikes,
                    SUM(f.comment_count) AS totalComments,
                    SUM(f.share_count)   AS totalShares,
                    COUNT(f.video_id)    AS totalVideos
                FROM {SOURCE_DB}.fact_video_interactions f
                JOIN {SOURCE_DB}.DateDim d
                        ON f.start_date_sk = d.date_sk
                GROUP BY d.date_sk, d.full_date, d.day_of_week
                ORDER BY d.full_date;
            """)

            logger.info("✔ Hai bảng aggregate đã được tạo thành công trong dbAgg.")

    except Exception as e:
        logger.exception("Lỗi khi tạo aggregate: %s", e)

    finally:
        source_conn.close()
        agg_conn.close()


# ---- Main ----
if __name__ == "__main__":
    logger.info("Bắt đầu tạo aggregate database...")
    init_aggregate_db()
    create_aggregate_tables()
    logger.info("Hoàn tất quá trình aggregate.")
