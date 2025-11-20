import os
import json
import glob
import time
import csv
from datetime import datetime
from collections import defaultdict

import pymysql
from dotenv import load_dotenv

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


def get_db_conn(db=None):
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=db or MYSQL_DB_STAGING,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
    )


def ensure_schema():
    """Create database and tables from SQL file."""
    if not os.path.exists(SQL_INIT_PATH):
        print(f"⚠️  SQL init file not found: {SQL_INIT_PATH}")
        return

    with open(SQL_INIT_PATH, "r", encoding="utf-8") as f:
        sql_content = f.read()

    root_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with root_conn.cursor() as cur:
            # Execute SQL statements separated by semicolons
            for statement in sql_content.split(";"):
                statement = statement.strip()
                if statement:
                    cur.execute(statement)
    finally:
        root_conn.close()


def safe_int(value, default=0):
    """Safely convert value to int."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def get_date_sk_from_date(cur, date_str):
    """Get date_sk from DateDim for given date string (YYYY-MM-DD format)."""
    if not date_str:
        return None
    
    try:
        # Extract date part from datetime string (YYYY-MM-DD HH:MM:SS)
        date_part = date_str.split(" ")[0] if " " in date_str else date_str
        
        sql = "SELECT date_sk FROM DateDim WHERE full_date = %s LIMIT 1"
        cur.execute(sql, (date_part,))
        result = cur.fetchone()
        return result["date_sk"] if result else None
    except:
        return None


def insert_load_log(cur, batch_id, table_name, operation, record_count, status, error_msg=None):
    """Insert load log entry."""
    sql = """
        INSERT INTO LoadLog (batch_id, table_name, operation_type, record_count, status, start_time, error_message)
        VALUES (%s, %s, %s, %s, %s, NOW(), %s)
    """
    cur.execute(sql, (batch_id, table_name, operation, record_count, status, error_msg))


def get_nested(obj, *keys, default=None):
    """Get nested value from dict."""
    cur = obj
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def convert_timestamp(ts):
    """Convert Unix timestamp to datetime string. Handle both timestamp and datetime."""
    if ts is None:
        return None
    
    # If already a string datetime format (2024-01-01 HH:MM:SS)
    if isinstance(ts, str):
        try:
            # Check if it's a valid datetime string
            if len(ts) > 10 and (' ' in ts or 'T' in ts):
                return ts
        except:
            pass
        
        # Try parsing as Unix timestamp string
        try:
            ts = int(float(ts))
        except:
            return None
    
    # Convert Unix timestamp to datetime
    try:
        dt = datetime.fromtimestamp(int(ts))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return None


def extract_record(item):
    """Extract record data from JSON item."""
    # Video ID
    video_id = (
        item.get("id")
        or get_nested(item, "video", "id")
        or get_nested(item, "itemInfo", "itemId")
    )

    # Author fields
    author_meta = item.get("authorMeta") or item.get("author") or {}
    author_id = (
        author_meta.get("id")
        or get_nested(item, "author", "id")
        or get_nested(item, "author", "secUid")
        or get_nested(item, "author", "uniqueId")
    )
    author_name = (
        author_meta.get("name")
        or author_meta.get("uniqueId")
        or get_nested(item, "author", "uniqueId")
    )
    avatar = (
        author_meta.get("avatar")
        or get_nested(item, "author", "avatarThumb")
        or get_nested(item, "author", "avatarMedium")
        or get_nested(item, "author", "avatarLarger")
    )

    # Video fields
    text = item.get("text") or item.get("desc") or get_nested(item, "itemInfo", "text")
    duration = safe_int(item.get("duration") or get_nested(item, "video", "duration"))
    create_time = convert_timestamp(item.get("createTime") or get_nested(item, "itemInfo", "createTime"))
    web_url = item.get("webVideoUrl") or item.get("shareUrl") or item.get("url")

    # Interaction stats
    stats = item.get("stats") or item.get("statistics") or {}
    digg = safe_int(item.get("diggCount") or stats.get("diggCount") or stats.get("likeCount"), 0)
    play = safe_int(item.get("playCount") or stats.get("playCount") or stats.get("playCountSum"), 0)
    share = safe_int(item.get("shareCount") or stats.get("shareCount"), 0)
    comment = safe_int(item.get("commentCount") or stats.get("commentCount"), 0)
    collect = safe_int(item.get("collectCount") or stats.get("collectCount") or stats.get("saveCount"), 0)

    return {
        "video_id": str(video_id) if video_id is not None else None,
        "author_id": str(author_id) if author_id is not None else None,
        "author_name": author_name,
        "avatar": avatar,
        "text": text,
        "duration": duration,
        "create_time": create_time,
        "web_url": web_url,
        "digg": digg,
        "play": play,
        "share": share,
        "comment": comment,
        "collect": collect,
    }


def upsert_author(cur, author_id, name, avatar, date_sk=None):
    """Upsert author record with date dimension."""
    sql = (
        "INSERT INTO Authors (authorID, Name, avatar, extract_date_sk) VALUES (%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE Name=VALUES(Name), avatar=VALUES(avatar), extract_date_sk=VALUES(extract_date_sk)"
    )
    cur.execute(sql, (author_id, name, avatar, date_sk))


def upsert_video(cur, video_id, author_id, text, duration, create_time, url, date_sk=None):
    """Upsert video record with date dimension."""
    sql = (
        "INSERT INTO Videos (videoID, authorID, TextContent, Duration, CreateTime, WebVideoUrl, create_date_sk) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE authorID=VALUES(authorID), TextContent=VALUES(TextContent), Duration=VALUES(Duration), "
        "CreateTime=VALUES(CreateTime), WebVideoUrl=VALUES(WebVideoUrl), create_date_sk=VALUES(create_date_sk)"
    )
    cur.execute(sql, (video_id, author_id, text, duration, create_time, url, date_sk))


def upsert_interactions(cur, video_id, digg, play, share, comment, collect, date_sk=None):
    """Upsert video interactions record with date dimension."""
    sql = (
        "INSERT INTO VideoInteractions (videoID, DiggCount, PlayCount, ShareCount, CommentCount, CollectCount, interaction_date_sk) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE DiggCount=VALUES(DiggCount), PlayCount=VALUES(PlayCount), ShareCount=VALUES(ShareCount), "
        "CommentCount=VALUES(CommentCount), CollectCount=VALUES(CollectCount), interaction_date_sk=VALUES(interaction_date_sk)"
    )
    cur.execute(sql, (video_id, digg, play, share, comment, collect, date_sk))


def load_file_to_db(path):
    """Load JSON file to database."""
    # Read file
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    # Try parse as JSON or JSON lines
    try:
        data = json.loads(text)
        is_lines = False
    except json.JSONDecodeError:
        is_lines = True

    # Prepare data list
    items = []
    if not is_lines:
        items = [data] if isinstance(data, dict) else data
    else:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except json.JSONDecodeError:
                    print(f"⚠️  Skipped malformed JSON line in {os.path.basename(path)}")
                    continue

    # Connect to database
    conn = get_db_conn()
    processed = 0
    inserted = 0
    updated = 0
    skipped = 0

    try:
        with conn.cursor() as cur:
            for item in items:
                rec = extract_record(item)

                # Skip if no video ID
                if not rec["video_id"]:
                    skipped += 1
                    continue

                # Check if video exists
                check_sql = "SELECT COUNT(*) as cnt FROM Videos WHERE videoID = %s"
                cur.execute(check_sql, (rec["video_id"],))
                exists = cur.fetchone()["cnt"] > 0

                # Upsert author if exists
                if rec["author_id"]:
                    upsert_author(cur, rec["author_id"], rec["author_name"], rec["avatar"], None)

                # Upsert video
                upsert_video(
                    cur,
                    rec["video_id"],
                    rec["author_id"],
                    rec["text"],
                    rec["duration"],
                    rec["create_time"],
                    rec["web_url"],
                    None,
                )

                # Upsert interactions
                upsert_interactions(
                    cur,
                    rec["video_id"],
                    rec["digg"],
                    rec["play"],
                    rec["share"],
                    rec["comment"],
                    rec["collect"],
                    None,
                )

                if exists:
                    updated += 1
                else:
                    inserted += 1
                processed += 1
    finally:
        conn.close()

    return {"processed": processed, "inserted": inserted, "updated": updated, "skipped": skipped}


def load_staging():
    """Load staging data from storage."""
    time.sleep(3)  # Wait for DB to stabilize
    print("🚀 Loading staging data from storage...")

    # Ensure schema exists
    ensure_schema()

    # Find all JSON files
    files = sorted(glob.glob(os.path.join(STORAGE_PATH, "*.json")))
    if not files:
        print("❌ No JSON files found in", STORAGE_PATH)
        return

    # Process each file
    total_processed = 0
    total_inserted = 0
    total_updated = 0
    total_skipped = 0

    for fp in files:
        try:
            result = load_file_to_db(fp)
            print(
                f"✅ Loaded {os.path.basename(fp)}: {result['processed']} processed "
                f"({result['inserted']} new, {result['updated']} updated, {result['skipped']} skipped)"
            )
            total_processed += result["processed"]
            total_inserted += result["inserted"]
            total_updated += result["updated"]
            total_skipped += result["skipped"]
        except Exception as e:
            print(f"❌ Failed to load {fp}: {e}")

    print("\n✅ Staging load completed!")
    print(f"  Total processed: {total_processed}")
    print(f"  New records: {total_inserted}")
    print(f"  Updated records: {total_updated}")
    print(f"  Skipped records: {total_skipped}")


def ensure_raw_schema():
    """Create database and raw JSON table from SQL file."""
    if not os.path.exists(SQL_INIT_PATH):
        print(f"⚠️  SQL init file not found: {SQL_INIT_PATH}")
        return

    with open(SQL_INIT_PATH, "r", encoding="utf-8") as f:
        sql_content = f.read()

    root_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with root_conn.cursor() as cur:
            # Execute SQL statements separated by semicolons
            for statement in sql_content.split(";"):
                statement = statement.strip()
                if statement:
                    cur.execute(statement)
    finally:
        root_conn.close()


def insert_raw(cur, filename, content_obj, source_line=None, status="success", error_msg=None):
    """Insert raw JSON record with load status."""
    sql = (
        "INSERT INTO RawJson (filename, content, loaded_at, load_status, error_message, source_line) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )
    loaded_at = datetime.now()
    cur.execute(sql, (filename, json.dumps(content_obj, ensure_ascii=False), loaded_at, status, error_msg, source_line))


def process_raw_file(conn, path):
    """Process single JSON file - parse and insert to raw JSON table."""
    results = {
        "filename": os.path.basename(path),
        "inserted": 0,
        "failed": 0,
        "status": "success",
        "error": None,
    }

    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    # Try parse as whole JSON
    try:
        data = json.loads(text)
        is_lines = False
    except json.JSONDecodeError:
        # try JSON lines
        is_lines = True

    try:
        with conn.cursor() as cur:
            if not is_lines:
                # Insert the whole JSON as one row
                try:
                    insert_raw(cur, results["filename"], data, None, "success")
                    results["inserted"] += 1
                except Exception as e:
                    insert_raw(cur, results["filename"], data, None, "failed", str(e))
                    results["failed"] += 1
            else:
                # Parse line by line
                with open(path, "r", encoding="utf-8") as f:
                    line_no = 0
                    for line in f:
                        line_no += 1
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                            insert_raw(cur, results["filename"], obj, line_no, "success")
                            results["inserted"] += 1
                        except json.JSONDecodeError as e:
                            print(f"⚠️  Skipping malformed JSON line {line_no} in {results['filename']}: {e}")
                            results["failed"] += 1
                            continue
            conn.commit()
    except Exception as e:
        conn.rollback()
        results["status"] = "failed"
        results["error"] = str(e)
        raise

    return results


def load_raw():
    """Load raw JSON files to RawJson table in dbStaging."""
    time.sleep(3)  # Wait for DB to stabilize
    print("🚀 Loading raw JSON data to RawJson table...")
    
    batch_id = f"raw_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Ensure schema exists
    ensure_raw_schema()

    # Find all JSON files
    files = sorted(glob.glob(os.path.join(STORAGE_PATH, "*.json")))
    if not files:
        print("❌ No JSON files found in", STORAGE_PATH)
        return

    # Process each file
    conn = get_db_conn()
    conn.autocommit = False
    
    total_inserted = 0
    total_failed = 0
    load_results = []

    for fp in files:
        try:
            result = process_raw_file(conn, fp)
            load_results.append(result)
            print(
                f"✅ {result['filename']}: {result['inserted']} inserted, {result['failed']} failed"
            )
            total_inserted += result["inserted"]
            total_failed += result["failed"]
        except Exception as e:
            print(f"❌ Failed to process {fp}: {e}")
            with conn.cursor() as cur:
                insert_raw(cur, os.path.basename(fp), {}, None, "failed", str(e))
            conn.commit()
            total_failed += 1

    # Log to LoadLog table
    try:
        with conn.cursor() as cur:
            status = "success" if total_failed == 0 else "partial_success"
            insert_load_log(cur, batch_id, "RawJson", "INSERT", total_inserted, status, 
                          f"Failed: {total_failed}" if total_failed > 0 else None)
            conn.commit()
    except Exception as e:
        print(f"⚠️  Failed to log to LoadLog: {e}")
    finally:
        conn.close()

    print(f"\n✅ Raw load completed!")
    print(f"  Total rows inserted: {total_inserted}")
    print(f"  Total rows failed: {total_failed}")
    print(f"\n📊 Load Summary:")
    for result in load_results:
        status_icon = "✅" if result["status"] == "success" else "❌"
        print(f"  {status_icon} {result['filename']}: {result['inserted']} success, {result['failed']} failed")


def load_date_dim_from_csv(csv_path):
    """Load Date_Dim table from CSV file (without header)."""
    if not os.path.exists(csv_path):
        print(f"❌ Date dimension file not found: {csv_path}")
        return {"loaded": 0, "updated": 0, "skipped": 0}

    print(f"🚀 Loading Date Dimension from {os.path.basename(csv_path)}...")
    
    batch_id = f"date_dim_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Define column names since CSV has no header
    fieldnames = [
        "date_sk", "full_date", "day_since_2005", "month_since_2005",
        "day_of_week", "calendar_month", "calendar_year", "calendar_year_month",
        "day_of_month", "day_of_year", "week_of_year_sunday", "year_week_sunday",
        "week_sunday_start", "week_of_year_monday", "year_week_monday",
        "week_monday_start", "quarter", "month_num", "holiday", "day_type"
    ]

    conn = get_db_conn()
    loaded = 0
    updated = 0
    skipped = 0

    try:
        with conn.cursor() as cur:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, fieldnames=fieldnames)
                if not reader.fieldnames:
                    print("❌ CSV file is empty or invalid")
                    return {"loaded": 0, "updated": 0, "skipped": 0}

                for row_num, row in enumerate(reader, start=1):
                    try:
                        # Extract columns from CSV
                        date_sk = int(row.get("date_sk", 0))
                        full_date = row.get("full_date", "")
                        day_since_2005 = int(row.get("day_since_2005", 0))
                        month_since_2005 = int(row.get("month_since_2005", 0))
                        day_of_week = row.get("day_of_week", "")
                        calendar_month = row.get("calendar_month", "")
                        calendar_year = row.get("calendar_year", "")
                        calendar_year_month = row.get("calendar_year_month", "")
                        day_of_month = int(row.get("day_of_month", 0))
                        day_of_year = int(row.get("day_of_year", 0))
                        week_of_year_sunday = int(row.get("week_of_year_sunday", 0))
                        year_week_sunday = row.get("year_week_sunday", "")
                        week_sunday_start = row.get("week_sunday_start", "")
                        week_of_year_monday = int(row.get("week_of_year_monday", 0))
                        year_week_monday = row.get("year_week_monday", "")
                        week_monday_start = row.get("week_monday_start", "")
                        holiday = row.get("holiday", "Non-Holiday")
                        day_type = row.get("day_type", "Weekday")

                        # Check if date exists
                        check_sql = "SELECT COUNT(*) as cnt FROM DateDim WHERE date_sk = %s"
                        cur.execute(check_sql, (date_sk,))
                        exists = cur.fetchone()["cnt"] > 0

                        # Upsert date dimension
                        upsert_sql = """
                            INSERT INTO DateDim (
                                date_sk, full_date, day_since_2005, month_since_2005,
                                day_of_week, calendar_month, calendar_year, calendar_year_month,
                                day_of_month, day_of_year, week_of_year_sunday, year_week_sunday,
                                week_sunday_start, week_of_year_monday, year_week_monday,
                                week_monday_start, holiday, day_type
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                                holiday=VALUES(holiday),
                                day_type=VALUES(day_type)
                        """
                        cur.execute(
                            upsert_sql,
                            (
                                date_sk, full_date, day_since_2005, month_since_2005,
                                day_of_week, calendar_month, calendar_year, calendar_year_month,
                                day_of_month, day_of_year, week_of_year_sunday, year_week_sunday,
                                week_sunday_start, week_of_year_monday, year_week_monday,
                                week_monday_start, holiday, day_type,
                            ),
                        )

                        if exists:
                            updated += 1
                        else:
                            loaded += 1

                    except Exception as e:
                        print(f"⚠️  Skipped row {row_num}: {e}")
                        skipped += 1
                        continue

        # Log to LoadLog table
        try:
            with conn.cursor() as cur:
                status = "success" if skipped == 0 else "partial_success"
                insert_load_log(cur, batch_id, "DateDim", "UPSERT", loaded + updated, status,
                              f"Skipped: {skipped}" if skipped > 0 else None)
            conn.commit()
        except Exception as e:
            print(f"⚠️  Failed to log to LoadLog: {e}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error loading date dimension: {e}")
    finally:
        conn.close()

    print(f"✅ Date Dimension load completed!")
    print(f"  New records loaded: {loaded}")
    print(f"  Existing records updated: {updated}")
    print(f"  Skipped records: {skipped}")

    return {"loaded": loaded, "updated": updated, "skipped": skipped}


def compare_and_load_incremental():
    """Compare new JSON data with existing data - only load new/changed records."""
    time.sleep(3)
    print("🚀 Starting incremental load with data comparison...")
    
    batch_id = f"incremental_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Ensure schema exists
    ensure_schema()

    # Find all JSON files
    files = sorted(glob.glob(os.path.join(STORAGE_PATH, "*.json")))
    if not files:
        print("❌ No JSON files found in", STORAGE_PATH)
        return

    # Get existing video IDs from database
    conn = get_db_conn()
    existing_videos = set()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT videoID FROM Videos")
            for row in cur.fetchall():
                existing_videos.add(str(row["videoID"]))
    finally:
        conn.close()

    print(f"📊 Found {len(existing_videos)} existing videos in database")

    # Process each file with comparison
    total_new = 0
    total_updated = 0
    total_skipped = 0

    for fp in files:
        try:
            # Read file
            with open(fp, "r", encoding="utf-8") as f:
                text = f.read()

            # Parse JSON
            try:
                data = json.loads(text)
                items = [data] if isinstance(data, dict) else data
            except json.JSONDecodeError:
                items = [json.loads(line) for line in text.split("\n") if line.strip()]

            # Filter and load only new/changed records
            conn = get_db_conn()
            new_count = 0
            updated_count = 0

            try:
                with conn.cursor() as cur:
                    for item in items:
                        rec = extract_record(item)

                        if not rec["video_id"]:
                            total_skipped += 1
                            continue

                        # Get date_sk from create_time
                        date_sk = get_date_sk_from_date(cur, rec["create_time"])

                        # Check if video exists
                        is_existing = rec["video_id"] in existing_videos

                        if not is_existing:
                            # New video - insert all related data
                            if rec["author_id"]:
                                upsert_author(cur, rec["author_id"], rec["author_name"], rec["avatar"], date_sk)

                            upsert_video(
                                cur,
                                rec["video_id"],
                                rec["author_id"],
                                rec["text"],
                                rec["duration"],
                                rec["create_time"],
                                rec["web_url"],
                                date_sk,
                            )

                            upsert_interactions(
                                cur,
                                rec["video_id"],
                                rec["digg"],
                                rec["play"],
                                rec["share"],
                                rec["comment"],
                                rec["collect"],
                                date_sk,
                            )

                            existing_videos.add(rec["video_id"])
                            new_count += 1
                        else:
                            # Existing video - only update interactions
                            upsert_interactions(
                                cur,
                                rec["video_id"],
                                rec["digg"],
                                rec["play"],
                                rec["share"],
                                rec["comment"],
                                rec["collect"],
                                date_sk,
                            )
                            updated_count += 1

                conn.commit()
                total_new += new_count
                total_updated += updated_count

                print(
                    f"✅ {os.path.basename(fp)}: {new_count} new, {updated_count} updated"
                )
            finally:
                conn.close()

        except Exception as e:
            print(f"❌ Failed to process {fp}: {e}")

    # Log to LoadLog table
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            insert_load_log(cur, batch_id, "Authors", "UPSERT", total_new, "success", None)
            insert_load_log(cur, batch_id, "Videos", "UPSERT", total_new, "success", None)
            insert_load_log(cur, batch_id, "VideoInteractions", "UPDATE", total_updated, "success", None)
        conn.commit()
    except Exception as e:
        print(f"⚠️  Failed to log to LoadLog: {e}")
    finally:
        conn.close()

    print("\n✅ Incremental load completed!")
    print(f"  New records: {total_new}")
    print(f"  Updated records: {total_updated}")
    print(f"  Skipped records: {total_skipped}")


if __name__ == "__main__":
    # Step 1: Load raw JSON data
    load_raw()

    # Step 2: Load Date Dimension (before incremental load)
    date_dim_result = load_date_dim_from_csv(DATE_DIM_PATH)
    print(f"📊 Date Dimension: {date_dim_result['loaded']} new, {date_dim_result['updated']} updated")

    # Step 3: Incremental load with comparison
    compare_and_load_incremental()
