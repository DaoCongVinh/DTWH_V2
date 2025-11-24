"""
Script to verify data after manual run
"""
import os
os.environ['MYSQL_HOST'] = 'localhost'
os.environ['MYSQL_PORT'] = '3306'
os.environ['MYSQL_USER'] = 'user'
os.environ['MYSQL_PASSWORD'] = 'dwhtiktok'
os.environ['MYSQL_DATABASE'] = 'dbStaging'

from db import DatabaseConnection

db = DatabaseConnection()
db.connect()
cursor = db.connection.cursor()

print("\n=== Database Verification ===")
cursor.execute('SELECT COUNT(*) FROM Authors')
print(f"Authors: {cursor.fetchone()[0]}")

cursor.execute('SELECT COUNT(*) FROM Videos')
print(f"Videos: {cursor.fetchone()[0]}")

cursor.execute('SELECT COUNT(*) FROM VideoInteractions')
print(f"VideoInteractions: {cursor.fetchone()[0]}")

cursor.execute('SELECT COUNT(*) FROM DateDim')
print(f"DateDim: {cursor.fetchone()[0]}")

cursor.execute('SELECT COUNT(*) FROM RawJson')
print(f"RawJson: {cursor.fetchone()[0]}")

print("\n=== Recent Authors ===")
cursor.execute('SELECT author_id, author_name FROM Authors ORDER BY created_at DESC LIMIT 3')
for row in cursor.fetchall():
    print(f"  - {row[1]} (ID: {row[0]})")

print("\n=== Recent Videos ===")
cursor.execute('SELECT video_id, description, view_count FROM Videos ORDER BY created_at DESC LIMIT 3')
for row in cursor.fetchall():
    desc = row[1][:50] + '...' if row[1] and len(row[1]) > 50 else (row[1] or 'N/A')
    print(f"  - Video ID {row[0]} (Views: {row[2]})")
    print(f"    Desc: {desc}")

cursor.close()
db.close()
print("\n=== Verification Complete ===\n")
