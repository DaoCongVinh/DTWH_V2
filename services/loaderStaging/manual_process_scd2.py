import pymysql
import yaml
import os
import sys
import json
from datetime import datetime

# Import the ETL processor
from etl_processor_scd2 import TikTokETLProcessor

def load_config():
    """Load database configuration"""
    # Database configuration (matching docker-compose)
    config = {
        'host': 'db',
        'user': 'user',
        'password': 'dwhtiktok',
        'database': 'dbStaging',
        'port': 3306
    }
    return config

def connect_to_db(config):
    """Establish database connection"""
    try:
        connection = pymysql.connect(**config)
        print("✅ Database connection established successfully!")
        return connection
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def main():
    print("🚀 Starting manual SCD Type 2 processing...")
    
    # Load configuration
    config = load_config()
    
    # Connect to database
    conn = connect_to_db(config)
    if not conn:
        return False
    
    try:
        # Initialize ETL processor
        etl = TikTokETLProcessor(config)
        
        # Get pending records from RawJson table
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        query = """
        SELECT id, content 
        FROM RawJson 
        WHERE load_status = 'PENDING'
        ORDER BY loaded_at
        """
        
        cursor.execute(query)
        pending_records = cursor.fetchall()
        
        if not pending_records:
            print("ℹ️  No pending records found to process")
            return True
            
        print(f"📊 Found {len(pending_records)} pending records to process")
        
        processed_count = 0
        error_count = 0
        
        for record in pending_records:
            try:
                raw_json_id = record['id']
                json_data_str = record['content']
                
                # Parse JSON data
                json_data = json.loads(json_data_str)
                
                print(f"⚙️  Processing record {raw_json_id}...")
                
                # Process with SCD Type 2 ETL
                success = etl.process_json_data(json_data, raw_json_id)
                
                if success:
                    # Update load status to PROCESSED
                    update_query = """
                    UPDATE RawJson 
                    SET load_status = 'PROCESSED' 
                    WHERE id = %s
                    """
                    cursor.execute(update_query, (raw_json_id,))
                    conn.commit()
                    
                    processed_count += 1
                    print(f"✅ Record {raw_json_id} processed successfully")
                else:
                    error_count += 1
                    print(f"❌ Failed to process record {raw_json_id}")
                    
            except Exception as e:
                error_count += 1
                print(f"❌ Error processing record {raw_json_id}: {e}")
                
        print(f"\n📈 Processing Summary:")
        print(f"   - Total processed: {processed_count}")
        print(f"   - Errors: {error_count}")
        
        # Show SCD Type 2 results
        print(f"\n🔍 Checking SCD Type 2 results...")
        
        # Check Authors table
        print(f"\n👤 Authors table (SCD Type 2):")
        cursor.execute("""
        SELECT AuthorID, username, followers_count, extract_date_sk, is_current
        FROM Authors 
        ORDER BY AuthorID, extract_date_sk DESC
        """)
        authors = cursor.fetchall()
        
        for author in authors:
            status = "CURRENT" if author['is_current'] else "HISTORIC"
            print(f"   AuthorID: {author['AuthorID']}, Username: {author['username']}, "
                  f"Followers: {author['followers_count']}, Date_SK: {author['extract_date_sk']}, "
                  f"Status: {status}")
        
        # Check Videos table
        print(f"\n📹 Videos table (SCD Type 2):")
        cursor.execute("""
        SELECT VideoID, play_count, like_count, extract_date_sk, is_current
        FROM Videos 
        ORDER BY VideoID, extract_date_sk DESC
        """)
        videos = cursor.fetchall()
        
        for video in videos:
            status = "CURRENT" if video['is_current'] else "HISTORIC"
            print(f"   VideoID: {video['VideoID']}, Plays: {video['play_count']}, "
                  f"Likes: {video['like_count']}, Date_SK: {video['extract_date_sk']}, "
                  f"Status: {status}")
        
        return True
        
    except Exception as e:
        print(f"❌ Processing failed: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)