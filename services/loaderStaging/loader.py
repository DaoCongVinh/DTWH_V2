import os
import glob
import json
import csv
import re
from datetime import datetime

import pymysql
from pymysql import OperationalError
from dotenv import load_dotenv
from etl_processor_scd2 import run_etl_pipeline

# Load environment
load_dotenv()

# Config defaults
MYSQL_HOST = os.getenv("MYSQL_HOST", "db")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB_STAGING = os.getenv("MYSQL_DATABASE_STAGING", "dbStaging")
STORAGE_PATH = os.getenv("STORAGE_PATH", "/data/storage")
SQL_INIT_PATH = os.getenv("SQL_INIT_PATH", "./loader.sql")
DATE_DIM_PATH = os.getenv("DATE_DIM_PATH", "./date_dim.csv")


# -----------------------------
# Helper functions
# -----------------------------

def procedure_exists(conn, name: str) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW PROCEDURE STATUS WHERE Db = DATABASE() AND Name = %s", (name,))
            return cur.fetchone() is not None
    except Exception:
        return False


def load_date_dim_fallback(conn, cur, csv_path: str):
    if not os.path.exists(csv_path):
        print(f"❌ Fallback: file không tồn tại: {csv_path}")
        return {"loaded": 0, "updated": 0, "skipped": 0}

    field_count_expected = 20
    loaded = 0
    updated = 0
    skipped = 0
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for line_no, row in enumerate(reader, start=1):
            if not row:
                continue
            if len(row) != field_count_expected:
                print(f"⚠️  Dòng {line_no}: số cột {len(row)} != {field_count_expected} -> bỏ qua")
                skipped += 1
                continue
            try:
                (
                    date_sk,
                    full_date,
                    day_since_2005,
                    month_since_2005,
                    day_of_week,
                    calendar_month,
                    calendar_year,
                    calendar_year_month,
                    day_of_month,
                    day_of_year,
                    week_of_year_sunday,
                    year_week_sunday,
                    week_sunday_start,
                    week_of_year_monday,
                    year_week_monday,
                    week_monday_start,
                    quarter_raw,
                    month_num,
                    holiday,
                    day_type,
                ) = row

                # Chuyển đổi quarter "2005-Q01" -> 1
                quarter_num = None
                if quarter_raw:
                    if "-Q" in quarter_raw:
                        try:
                            quarter_part = quarter_raw.split("-Q")[-1]
                            quarter_num = int(quarter_part)
                        except ValueError:
                            quarter_num = 0
                    else:
                        try:
                            quarter_num = int(quarter_raw)
                        except ValueError:
                            quarter_num = 0
                else:
                    quarter_num = 0

                # Kiểm tra tồn tại
                cur.execute("SELECT COUNT(*) AS cnt FROM DateDim WHERE date_sk=%s", (int(date_sk),))
                exists = cur.fetchone()["cnt"] > 0

                upsert_sql = """
                    INSERT INTO DateDim (
                        date_sk, full_date, day_since_2005, month_since_2005,
                        day_of_week, calendar_month, calendar_year, calendar_year_month,
                        day_of_month, day_of_year, week_of_year_sunday, year_week_sunday,
                        week_sunday_start, week_of_year_monday, year_week_monday,
                        week_monday_start, quarter, month_num, holiday, day_type
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        full_date=VALUES(full_date),
                        day_since_2005=VALUES(day_since_2005),
                        month_since_2005=VALUES(month_since_2005),
                        day_of_week=VALUES(day_of_week),
                        calendar_month=VALUES(calendar_month),
                        calendar_year=VALUES(calendar_year),
                        calendar_year_month=VALUES(calendar_year_month),
                        day_of_month=VALUES(day_of_month),
                        day_of_year=VALUES(day_of_year),
                        week_of_year_sunday=VALUES(week_of_year_sunday),
                        year_week_sunday=VALUES(year_week_sunday),
                        week_sunday_start=VALUES(week_sunday_start),
                        week_of_year_monday=VALUES(week_of_year_monday),
                        year_week_monday=VALUES(year_week_monday),
                        week_monday_start=VALUES(week_monday_start),
                        quarter=VALUES(quarter),
                        month_num=VALUES(month_num),
                        holiday=VALUES(holiday),
                        day_type=VALUES(day_type)
                """
                cur.execute(
                    upsert_sql,
                    (
                        int(date_sk),
                        full_date,
                        int(day_since_2005),
                        int(month_since_2005),
                        day_of_week,
                        calendar_month,
                        calendar_year,
                        calendar_year_month,
                        int(day_of_month),
                        int(day_of_year),
                        int(week_of_year_sunday),
                        year_week_sunday,
                        week_sunday_start,
                        int(week_of_year_monday),
                        year_week_monday,
                        week_monday_start,
                        int(quarter_num),
                        int(month_num),
                        holiday,
                        day_type,
                    ),
                )
                if exists:
                    updated += 1
                else:
                    loaded += 1
            except Exception as e:
                print(f"⚠️  Dòng {line_no} lỗi: {e}")
                skipped += 1
                continue
    return {"loaded": loaded, "updated": updated, "skipped": skipped}


def load_date_dim_once_if_empty(conn, cur, csv_path: str):
    """
    Chỉ load DateDim một lần duy nhất nếu table còn trống
    """
    # Check if DateDim already has data
    cur.execute("SELECT COUNT(*) AS cnt FROM DateDim")
    existing_count = cur.fetchone()["cnt"]
    
    if existing_count > 0:
        print(f"✅ DateDim đã có {existing_count} records. Bỏ qua việc load lại.")
        return {"total_rows": existing_count, "loaded": 0, "updated": 0, "skipped": existing_count, "already_loaded": True}
    
    print("🔄 DateDim trống, bắt đầu load dữ liệu lần đầu...")
    
    # Try stored procedure first
    proc_name = "load_date_dim_from_csv"
    used_fallback = False
    result_info = {}
    
    if procedure_exists(conn, proc_name):
        print("🚀 Gọi thủ tục load_date_dim_from_csv ...")
        try:
            cur.callproc(proc_name, (csv_path,))
            # Sau khi thủ tục chạy, đếm số dòng
            cur.execute("SELECT COUNT(*) AS cnt FROM DateDim")
            cnt = cur.fetchone()["cnt"]
            print(f"✅ Thủ tục chạy xong. Tổng dòng DateDim: {cnt}")
            result_info = {"total_rows": cnt, "fallback": False, "loaded": cnt, "updated": 0, "skipped": 0}
        except OperationalError as e:
            print(f"❌ Lỗi khi gọi thủ tục: {e}. Dùng fallback Python.")
            used_fallback = True
        except Exception as e:
            print(f"❌ Lỗi không xác định khi gọi thủ tục: {e}. Dùng fallback Python.")
            used_fallback = True
    else:
        print("⚠️  Thủ tục load_date_dim_from_csv không tồn tại. Dùng fallback Python.")
        used_fallback = True

    if used_fallback:
        stats = load_date_dim_fallback(conn, cur, csv_path)
        cur.execute("SELECT COUNT(*) AS cnt FROM DateDim")
        cnt = cur.fetchone()["cnt"]
        print(
            f"✅ Fallback hoàn tất. New: {stats['loaded']}, Updated: {stats['updated']}, Skipped: {stats['skipped']} | Tổng: {cnt}"
        )
        result_info = {"total_rows": cnt, "fallback": True, **stats}

    # Ghi log nếu thủ tục insert_load_log tồn tại
    if procedure_exists(conn, "insert_load_log"):
        try:
            batch_id = f"date_dim_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            record_count = result_info.get("total_rows", 0)
            cur.callproc(
                "insert_load_log",
                (
                    batch_id,
                    "DateDim",
                    "UPSERT",
                    record_count,
                    "success",
                    None,
                ),
            )
            print("📝 Đã ghi log DateDim vào LoadLog.")
        except Exception as e:
            print(f"⚠️  Không thể ghi LoadLog cho DateDim: {e}")
    
    return result_info


# -----------------------------
# Main functions
# -----------------------------


def get_db_conn(db=None):
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=db or MYSQL_DB_STAGING,
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
        local_infile=True,
    )


def execute_sql_script(conn, path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"SQL script not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        script = f.read()

    # Split by procedure blocks and regular statements
    procedures = []
    current_proc = ""
    in_procedure = False
    regular_statements = []
    current_statement = ""
    
    for line in script.split('\n'):
        line_stripped = line.strip()
        
        # Skip comments and empty lines
        if not line_stripped or line_stripped.startswith('--'):
            continue
            
        # Detect procedure start (only CREATE PROCEDURE, not DROP PROCEDURE)
        if line_stripped.upper().startswith('CREATE PROCEDURE'):
            if current_statement:
                regular_statements.append(current_statement.strip())
                current_statement = ""
            in_procedure = True
            current_proc = line + '\n'
            continue
            
        if in_procedure:
            current_proc += line + '\n'
            # End procedure at standalone END;
            if line_stripped.upper() == 'END;':
                procedures.append(current_proc.strip())
                current_proc = ""
                in_procedure = False
            continue
        
        # Regular statement processing
        current_statement += line + '\n'
        if line_stripped.endswith(';'):
            regular_statements.append(current_statement.strip())
            current_statement = ""
    
    if current_statement.strip():
        regular_statements.append(current_statement.strip())
    if current_proc.strip():
        procedures.append(current_proc.strip())

    with conn.cursor() as cur:
        # Separate DROP statements from other regular statements
        drop_statements = []
        other_statements = []
        
        for statement in regular_statements:
            if statement.strip():
                if statement.strip().upper().startswith('DROP PROCEDURE'):
                    drop_statements.append(statement)
                else:
                    other_statements.append(statement)
        
        # Execute DROP PROCEDURE statements first
        for statement in drop_statements:
            try:
                cur.execute(statement)
            except Exception as e:
                # DROP PROCEDURE IF EXISTS shouldn't error, but if it does, just continue
                continue
        
        # Execute other regular statements
        for statement in other_statements:
            if statement.strip():
                try:
                    cur.execute(statement)
                except Exception as e:
                    print(f"⚠️  Warning executing statement: {e}")
                    continue
                    
        # Execute CREATE PROCEDURE statements
        for proc in procedures:
            if proc.strip():
                try:
                    cur.execute(proc)
                except Exception as e:
                    # Suppress warning if procedure already exists (shouldn't happen after DROP)
                    error_str = str(e)
                    if 'already exists' in error_str.lower() or '1304' in error_str:
                        # Try to extract procedure name and drop it first, then recreate
                        match = re.search(r'CREATE\s+PROCEDURE\s+(\w+)', proc, re.IGNORECASE)
                        if match:
                            proc_name = match.group(1)
                            try:
                                cur.execute(f"DROP PROCEDURE IF EXISTS {proc_name}")
                                cur.execute(proc)
                                continue
                            except Exception as retry_err:
                                # If retry fails, suppress the warning
                                continue
                    print(f"⚠️  Warning executing procedure: {e}")
                    print(f"Procedure: {proc[:100]}...")
                    continue
                    
    conn.commit()


def run_pipeline():
    conn = get_db_conn()
    try:
        execute_sql_script(conn, SQL_INIT_PATH)
        print("✅ Schema + procedures ready.")
        
        # Ensure extract_date_sk columns exist in all tables
        with conn.cursor() as cur:
            if procedure_exists(conn, "ensure_extract_date_sk_columns"):
                try:
                    cur.callproc("ensure_extract_date_sk_columns")
                    conn.commit()
                    print("✅ Updated table schemas with extract_date_sk columns.")
                except Exception as e:
                    print(f"⚠️  Warning updating schemas: {e}")
                    conn.rollback()

        files = sorted(glob.glob(os.path.join(STORAGE_PATH, "*.json")))
        if not files:
            print("❌ No JSON files found in", STORAGE_PATH)
        else:
            batch_id = f"raw_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            processed = 0
            errors = 0
            error_messages = {}  # Track unique errors to avoid duplicate logs
            
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                # Check which files have already been processed
                cur.execute("SELECT DISTINCT filename FROM RawJson WHERE load_status IN ('success', 'processed')")
                processed_files = {row['filename'] for row in cur.fetchall()}
                
                for fp in files:
                    filename = os.path.basename(fp)
                    
                    # Skip if already processed
                    if filename in processed_files:
                        continue
                    
                    try:
                        with open(fp, "r", encoding="utf-8") as f:
                            content = f.read().strip()
                            if not content:
                                continue
                            
                            # Try to parse as JSON array first
                            try:
                                json_data = json.loads(content)
                                if isinstance(json_data, list):
                                    # Process each object in array
                                    for idx, obj in enumerate(json_data, start=1):
                                        payload = json.dumps(obj, ensure_ascii=False)
                                        try:
                                            cur.callproc("process_raw_record", (filename, payload, idx))
                                            processed += 1
                                        except Exception as err:
                                            conn.rollback()
                                            errors += 1
                                            # Only log unique errors once
                                            error_key = f"{filename}:{str(err)[:100]}"
                                            if error_key not in error_messages:
                                                error_messages[error_key] = True
                                                print(f"❌ {filename}:{idx} -> {err}")
                                else:
                                    # Single JSON object
                                    payload = json.dumps(json_data, ensure_ascii=False)
                                    try:
                                        cur.callproc("process_raw_record", (filename, payload, 1))
                                        processed += 1
                                    except Exception as err:
                                        conn.rollback()
                                        errors += 1
                                        # Only log unique errors once
                                        error_key = f"{filename}:{str(err)[:100]}"
                                        if error_key not in error_messages:
                                            error_messages[error_key] = True
                                            print(f"❌ {filename}:1 -> {err}")
                            except json.JSONDecodeError as e:
                                errors += 1
                                error_key = f"{filename}:JSON_ERROR"
                                if error_key not in error_messages:
                                    error_messages[error_key] = True
                                    print(f"❌ Invalid JSON in {filename}: {e}")
                    except Exception as file_err:
                        errors += 1
                        error_key = f"{filename}:FILE_ERROR"
                        if error_key not in error_messages:
                            error_messages[error_key] = True
                            print(f"❌ Error reading {filename}: {file_err}")
                    conn.commit()
            with conn.cursor() as cur:
                cur.callproc(
                    "insert_load_log",
                    (
                        batch_id,
                        "RawJson",
                        "INSERT",
                        processed,
                        "success" if errors == 0 else "partial_success",
                        None,
                    ),
                )
            conn.commit()

            with conn.cursor() as cur:
                load_date_dim_once_if_empty(conn, cur, DATE_DIM_PATH)
            conn.commit()

            # Run ETL Process to extract data into structured tables
            try:
                run_etl_pipeline(conn)
            except Exception as e:
                print(f"❌ ETL Process failed: {e}")
                # Continue even if ETL fails, raw data is still loaded

            print(f"\n📊 Finished raw load ({processed} rows, {errors} errors).")
    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()
