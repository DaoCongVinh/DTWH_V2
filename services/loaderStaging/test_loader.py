#!/usr/bin/env python3
"""Test script để kiểm tra các hàm loader và debug các vấn đề."""

import os
import sys
sys.path.append('.')

from loader import (
    get_db_conn, 
    procedure_exists, 
    load_date_dim_fallback, 
    load_date_dim_with_proc_or_fallback
)

def test_connection():
    """Test kết nối database."""
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION() as version")
            result = cur.fetchone()
            print(f"✅ Kết nối database thành công: {result['version']}")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Lỗi kết nối database: {e}")
        return False

def test_schema_exists():
    """Kiểm tra schema có tồn tại không."""
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_schema = 'dbStaging'")
            result = cur.fetchone()
            table_count = result['cnt']
            print(f"📊 Database dbStaging có {table_count} bảng")
            
            # Liệt kê các bảng
            cur.execute("SHOW TABLES")
            tables = [row[list(row.keys())[0]] for row in cur.fetchall()]
            print(f"📋 Các bảng: {', '.join(tables)}")
        conn.close()
        return table_count > 0
    except Exception as e:
        print(f"❌ Lỗi kiểm tra schema: {e}")
        return False

def test_procedures():
    """Kiểm tra các thủ tục có tồn tại không."""
    try:
        conn = get_db_conn()
        procedures = ['insert_load_log', 'process_raw_record', 'load_date_dim_from_csv']
        for proc in procedures:
            exists = procedure_exists(conn, proc)
            status = "✅" if exists else "❌"
            print(f"{status} Thủ tục {proc}: {'có' if exists else 'không có'}")
        conn.close()
    except Exception as e:
        print(f"❌ Lỗi kiểm tra thủ tục: {e}")

def test_date_dim_fallback():
    """Test fallback load DateDim."""
    csv_path = "./date_dim.csv"
    if not os.path.exists(csv_path):
        print(f"❌ File CSV không tồn tại: {csv_path}")
        return
    
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            # Kiểm tra bảng DateDim trống không
            cur.execute("SELECT COUNT(*) as cnt FROM DateDim")
            before_count = cur.fetchone()['cnt']
            print(f"📊 DateDim có {before_count} dòng trước khi load")
            
            # Test fallback
            stats = load_date_dim_fallback(conn, cur, csv_path)
            conn.commit()
            
            # Kiểm tra sau khi load
            cur.execute("SELECT COUNT(*) as cnt FROM DateDim")
            after_count = cur.fetchone()['cnt']
            print(f"📊 DateDim có {after_count} dòng sau khi load")
            print(f"📊 Stats: {stats}")
            
            # Kiểm tra vài mẫu dữ liệu
            cur.execute("SELECT date_sk, full_date, quarter, calendar_year FROM DateDim LIMIT 5")
            samples = cur.fetchall()
            print("📋 Dữ liệu mẫu:")
            for row in samples:
                print(f"  {row['date_sk']}: {row['full_date']} Q{row['quarter']} {row['calendar_year']}")
        
        conn.close()
    except Exception as e:
        print(f"❌ Lỗi test fallback: {e}")

def main():
    print("🚀 Bắt đầu test loader components...")
    
    if not test_connection():
        return
    
    if not test_schema_exists():
        print("⚠️  Schema chưa tồn tại, sẽ cần chạy loader để tạo")
    
    test_procedures()
    print("\n" + "="*50)
    print("📝 Test fallback load DateDim:")
    test_date_dim_fallback()

if __name__ == "__main__":
    main()