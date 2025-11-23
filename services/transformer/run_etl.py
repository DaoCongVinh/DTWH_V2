import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_ROOT_USER = os.getenv("MYSQL_ROOT_USER")
MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD")
WAREHOUSE_DB_NAME = os.getenv("DB_WAREHOUSE")

def run_etl():
    print(f"Connecting to MySQL server {MYSQL_HOST}:{MYSQL_PORT} ...")

    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_ROOT_USER,
        password=MYSQL_ROOT_PASSWORD,
        database=WAREHOUSE_DB_NAME
    )

    cur = conn.cursor()
    print("Running ETL procedure: etl_tiktok_procedure()")

    cur.execute("CALL etl_tiktok_procedure();")
    conn.commit()

    cur.close()
    conn.close()
    print("ETL executed successfully!")

if __name__ == "__main__":
    run_etl()
