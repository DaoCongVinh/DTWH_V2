import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_ROOT_USER = os.getenv("MYSQL_ROOT_USER")
MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD")
WAREHOUSE_DB_NAME = os.getenv("DB_WAREHOUSE")  # vd: warehouse_tiktok

def get_active_procedure(cursor):
    """
    Lấy procedure đang active trong bảng metadata.
    """
    cursor.execute("""
        SELECT procedure_name 
        FROM metadata_tiktok.config_etl 
        WHERE is_active = 1 
        ORDER BY updated_at DESC 
        LIMIT 1;
    """)
    row = cursor.fetchone()
    return row[0] if row else None

def run_etl():
    print(f"Connecting to MySQL server {MYSQL_HOST}:{MYSQL_PORT} ...")

    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_ROOT_USER,
        password=MYSQL_ROOT_PASSWORD,
        db=WAREHOUSE_DB_NAME,
    )

    cur = conn.cursor()

    # ⭐ Lấy tên procedure đang active
    procedure_name = get_active_procedure(cur)

    if procedure_name is None:
        print("ERROR: Không tìm thấy ETL procedure nào đang active trong metadata_tiktok.config_etl.")
        return

    print(f"Running ETL procedure: {procedure_name}()")

    sql = f"CALL {procedure_name}();"
    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()

    print("ETL executed successfully!")

if __name__ == "__main__":
    run_etl()
z