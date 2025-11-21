#!/usr/bin/env python3
"""
Test ETL Processor independently
"""

import os
import json
import pymysql
import pymysql.cursors
from datetime import datetime
from dotenv import load_dotenv

# Load environment
load_dotenv()

def test_etl_standalone():
    """Test ETL processor với một sample record"""
    
    conn = pymysql.connect(
        host='db',
        user=os.getenv('MYSQL_USER', 'user'),
        password=os.getenv('MYSQL_PASSWORD', 'dwhtiktok'),
        database='dbStaging',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        # Get one sample record
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, filename, content 
                FROM RawJson 
                WHERE load_status = 'PROCESSED'
                LIMIT 1
            """)
            sample = cursor.fetchone()
            
            if sample:
                print("📄 Sample Record:")
                print(f"ID: {sample['id']}")
                print(f"Filename: {sample['filename']}")
                
                # Parse JSON content
                json_content = sample['content']
                if isinstance(json_content, str):
                    json_content = json.loads(json_content)
                    
                print(f"JSON Keys: {list(json_content.keys())}")
                
                # Test Author extraction
                author_data = json_content.get('authorMeta', {})
                print(f"\n👤 Author Data:")
                print(f"  Full authorMeta keys: {list(author_data.keys()) if author_data else 'None'}")
                print(f"  ID: {author_data.get('id') if author_data else 'No authorMeta'}")
                print(f"  Nickname: {author_data.get('nickname') if author_data else 'No authorMeta'}")
                print(f"  Avatar: {author_data.get('avatar', '')[:50] if author_data else 'No authorMeta'}...")
                
                # Also test direct field access
                print(f"  Direct JSON keys with 'author' or 'Author': {[k for k in json_content.keys() if 'uthor' in k.lower()]}")
                
                # Check top-level fields
                print(f"  Top level 'id': {json_content.get('id')}")
                print(f"  Top level 'text': {json_content.get('text', '')[:50]}...")
                print(f"  Top level 'webVideoUrl': {json_content.get('webVideoUrl', '')[:50]}...")
                
                # Test Video data
                print(f"\n🎬 Video Data:")
                print(f"  ID: {json_content.get('id')}")
                print(f"  Desc: {json_content.get('desc', '')[:50]}...")
                print(f"  CreateTime: {json_content.get('createTime')}")
                
                # Test Stats data
                stats = json_content.get('stats', {})
                print(f"\n📊 Stats Data:")
                print(f"  DiggCount: {stats.get('diggCount', 0)}")
                print(f"  PlayCount: {stats.get('playCount', 0)}")
                print(f"  ShareCount: {stats.get('shareCount', 0)}")
                
                # Test date_sk
                from etl_processor import TikTokETLProcessor
                processor = TikTokETLProcessor(conn)
                date_sk = processor.get_date_sk_for_today()
                print(f"\n📅 Date SK: {date_sk}")
                
            else:
                print("❌ No sample records found")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    test_etl_standalone()