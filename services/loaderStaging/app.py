# app.py
import time
import pymysql
import config
from main_job import job  # Import job chính

def check_new_crawl_data():
    """
    Check if there are any successful crawl jobs in metadata_tiktok
    that haven't been loaded into dbStaging yet.
    """
    try:
        # 1. Get recent successful crawls from metadata_tiktok
        # We use a separate connection for metadata
        conn_meta = pymysql.connect(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            db="metadata_tiktok",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10
        )
        
        recent_crawls = []
        with conn_meta:
            with conn_meta.cursor() as cur:
                # Check last 10 successful crawls
                cur.execute("""
                    SELECT file_name 
                    FROM control_log 
                    WHERE status = 'SUCCESS' 
                    ORDER BY id DESC 
                    LIMIT 10
                """)
                rows = cur.fetchall()
                recent_crawls = [r['file_name'] for r in rows]
        
        if not recent_crawls:
            return False

        # 2. Check if these files exist in dbStaging.RawJson
        conn_staging = pymysql.connect(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            db=config.MYSQL_DB_STAGING,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10
        )
        
        with conn_staging:
            with conn_staging.cursor() as cur:
                if not recent_crawls:
                    return False
                    
                format_strings = ','.join(['%s'] * len(recent_crawls))
                cur.execute(f"SELECT filename FROM RawJson WHERE filename IN ({format_strings})", tuple(recent_crawls))
                loaded_files = {r['filename'] for r in cur.fetchall()}
        
        # 3. If any crawl file is NOT in loaded_files, we need to run
        for crawl_file in recent_crawls:
            if crawl_file not in loaded_files:
                print(f"🚀 Found new crawl file: {crawl_file}")
                return True
                
        return False

    except Exception as e:
        print(f"⚠️ Error checking for new data: {e}")
        return False

def start_polling():
    """
    Polls the database for new crawl data instead of using a fixed schedule.
    """
    print("🚀 Loader Service started in POLLING mode.")
    print("ℹ️  Waiting for 'SUCCESS' signal from Crawler in metadata_tiktok...")

    # Run once on startup to catch up any missed jobs
    if check_new_crawl_data():
        print("✅ New data detected on startup! Starting Loader Job...")
        job()

    while True:
        try:
            if check_new_crawl_data():
                print("✅ New data detected! Starting Loader Job...")
                job()
                print("🏁 Loader Job finished. Returning to polling...")
            
            # Sleep for 1 minute before next check
            time.sleep(60)
            
        except KeyboardInterrupt:
            print("⏹️  Polling stopped.")
            break
        except Exception as e:
            print(f"❌ Unexpected error in polling loop: {e}")
            time.sleep(60) # Sleep even on error to avoid tight loop

if __name__ == "__main__":
    start_polling()

