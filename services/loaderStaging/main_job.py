import os
import glob
import json
from datetime import datetime
import pymysql
from loader import (
    get_db_conn,
    execute_sql_script,
    procedure_exists,
    load_date_dim_once_if_empty,
    SQL_INIT_PATH,
    STORAGE_PATH,
    DATE_DIM_PATH,
    MYSQL_DB_STAGING
)
from etl_processor_scd2 import run_etl_pipeline

def job():
    """Pipeline chính: Load và transform dữ liệu từ JSON files vào staging database."""
    current_step = "Init"
    conn = None

    try:
        # --- BƯỚC 1: KẾT NỐI DB & SETUP SCHEMA ---
        current_step = "Connect Database & Setup Schema"
        print("🚀 Job started. Connecting to DB...")
        
        conn = get_db_conn()
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

        # --- BƯỚC 1.5: LOAD DATE DIMENSION FIRST (before processing any data) ---
        current_step = "Load Date Dimension"
        with conn.cursor() as cur:
            load_date_dim_once_if_empty(conn, cur, DATE_DIM_PATH)
        conn.commit()
        print("✅ DateDim ready for foreign key references.")

        # --- BƯỚC 2: LOAD RAW JSON FILES ---
        current_step = "Load Raw JSON Files"
        files = sorted(glob.glob(os.path.join(STORAGE_PATH, "*.json")))
        
        if not files:
            print(f"ℹ️  No JSON files found in {STORAGE_PATH}")
            return
        
        print(f"📂 Found {len(files)} JSON files to process...")
        
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
        
        # Log batch results
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

        # --- BƯỚC 3: RUN ETL PROCESS ---
        current_step = "ETL Process"
        try:
            run_etl_pipeline(conn)
            print("✅ ETL Process completed successfully.")
        except Exception as e:
            print(f"❌ ETL Process failed: {e}")
            # Continue even if ETL fails, raw data is still loaded

        print(f"\n📊 Finished raw load ({processed} rows, {errors} errors).")
        print("✅ Job finished successfully.")

    except Exception as e:
        print(f"❌ Job Failed at step: {current_step}")
        print(f"Error: {str(e)}")
        raise

    finally:
        if conn:
            conn.close()