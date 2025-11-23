#!/usr/bin/env python3
"""
Check database status and SCD Type 2 implementation
"""

import pymysql
import pymysql.cursors

# Database configuration
DB_CONFIG = {
    'host': 'db',
    'user': 'user',
    'password': 'dwhtiktok',
    'database': 'dbStaging',
    'cursorclass': pymysql.cursors.DictCursor
}

def check_database_status():
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print('📊 Database connection established')
        
        with connection.cursor() as cursor:
            # Check RawJson records status
            cursor.execute('SELECT load_status, COUNT(*) as count FROM RawJson GROUP BY load_status')
            status_counts = cursor.fetchall()
            print('🗂️  RawJson records by status:')
            for row in status_counts:
                print(f'   {row["load_status"]}: {row["count"]} records')
            
            # Check if we have any SCD Type 2 records
            cursor.execute('SELECT COUNT(*) as count FROM Authors')
            author_count = cursor.fetchone()
            print(f'👤 Authors table: {author_count["count"]} records')
            
            cursor.execute('SELECT COUNT(*) as count FROM Videos') 
            video_count = cursor.fetchone()
            print(f'📹 Videos table: {video_count["count"]} records')
            
            cursor.execute('SELECT COUNT(*) as count FROM VideoInteractions')
            interaction_count = cursor.fetchone()
            print(f'❤️  VideoInteractions table: {interaction_count["count"]} records')
            
            # Show sample SCD Type 2 data if exists
            if author_count["count"] > 0:
                print('\n📋 Sample Authors data (SCD Type 2):')
                cursor.execute('''
                SELECT author_id, author_name, is_current, extract_date_sk, 
                       DATE(CONCAT(SUBSTRING(extract_date_sk, 1, 4), '-', 
                                  SUBSTRING(extract_date_sk, 5, 2), '-', 
                                  SUBSTRING(extract_date_sk, 7, 2))) as extract_date
                FROM Authors 
                ORDER BY author_id, extract_date_sk 
                LIMIT 10
                ''')
                authors = cursor.fetchall()
                for author in authors:
                    print(f'   ID: {author["author_id"]} | Name: {author["author_name"][:20]}... | Current: {author["is_current"]} | Date: {author["extract_date"]}')
            
            if video_count["count"] > 0:
                print('\n📋 Sample Videos data (SCD Type 2):')
                cursor.execute('''
                SELECT video_id, video_desc, is_current, extract_date_sk,
                       DATE(CONCAT(SUBSTRING(extract_date_sk, 1, 4), '-', 
                                  SUBSTRING(extract_date_sk, 5, 2), '-', 
                                  SUBSTRING(extract_date_sk, 7, 2))) as extract_date
                FROM Videos 
                ORDER BY video_id, extract_date_sk 
                LIMIT 5
                ''')
                videos = cursor.fetchall()
                for video in videos:
                    desc = video["video_desc"][:30] + '...' if video["video_desc"] and len(video["video_desc"]) > 30 else video["video_desc"]
                    print(f'   ID: {video["video_id"]} | Desc: {desc} | Current: {video["is_current"]} | Date: {video["extract_date"]}')

    finally:
        if 'connection' in locals():
            connection.close()
            print('✅ Database connection closed')

if __name__ == "__main__":
    check_database_status()