"""
ETL Processor for TikTok Data
Extract từ RawJson -> Transform -> Load vào Authors, Videos, VideoInteractions
"""

import json
import logging
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional
import pymysql.cursors

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TikTokETLProcessor:
    def __init__(self, connection):
        self.conn = connection
        self.processed_records = 0
        self.inserted_records = 0
        self.updated_records = 0
        self.error_records = 0
        
    def get_date_sk_for_today(self) -> int:
        """Lấy date_sk từ DateDim cho ngày hôm nay"""
        today_str = date.today().strftime('%Y-%m-%d')
        
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT date_sk FROM DateDim WHERE full_date = %s LIMIT 1",
                (today_str,)
            )
            result = cursor.fetchone()
            
            if result:
                return result['date_sk']
            else:
                # Nếu không tìm thấy, tạo date_sk tạm thời dựa trên ngày
                # Format: YYYYMMDD
                return int(today_str.replace('-', ''))

    def extract_author_data(self, json_content: dict, extract_date_sk: int) -> Optional[Dict]:
        """Extract thông tin Author từ JSON"""
        try:
            author_data = json_content.get('authorMeta', {})
            if not author_data:
                return None
                
            return {
                'authorID': int(author_data.get('id', 0)),
                'Name': author_data.get('nickName', '').strip(),
                'avatar': author_data.get('avatar', '').strip(),
                'extract_date_sk': extract_date_sk
            }
        except Exception as e:
            logger.warning(f"Error extracting author data: {e}")
            return None

    def extract_video_data(self, json_content: dict, create_date_sk: int) -> Optional[Dict]:
        """Extract thông tin Video từ JSON"""
        try:
            video_data = json_content
            author_data = json_content.get('authorMeta', {})
            
            if not video_data.get('id'):
                return None
                
            return {
                'videoID': int(video_data.get('id', 0)),
                'authorID': int(author_data.get('id', 0)) if author_data.get('id') else None,
                'TextContent': video_data.get('text', '').strip()[:500],  # Limit text length
                'Duration': int(video_data.get('videoMeta', {}).get('duration', 0)),
                'CreateTime': self.parse_create_time(video_data.get('createTime')),
                'WebVideoUrl': video_data.get('webVideoUrl', '').strip(),
                'create_date_sk': create_date_sk
            }
        except Exception as e:
            logger.warning(f"Error extracting video data: {e}")
            return None

    def extract_interaction_data(self, json_content: dict, interaction_date_sk: int) -> Optional[Dict]:
        """Extract thông tin VideoInteractions từ JSON"""
        try:
            # Stats are directly in the main JSON object, not nested
            if not json_content.get('id'):
                return None
                
            return {
                'videoID': int(json_content.get('id', 0)),
                'DiggCount': int(json_content.get('diggCount', 0)),
                'PlayCount': int(json_content.get('playCount', 0)),
                'ShareCount': int(json_content.get('shareCount', 0)),
                'CommentCount': int(json_content.get('commentCount', 0)),
                'CollectCount': int(json_content.get('collectCount', 0)),
                'interaction_date_sk': interaction_date_sk
            }
        except Exception as e:
            logger.warning(f"Error extracting interaction data: {e}")
            return None

    def parse_create_time(self, timestamp) -> datetime:
        """Parse timestamp thành datetime"""
        try:
            if timestamp:
                return datetime.fromtimestamp(int(timestamp))
            else:
                return datetime.now()
        except:
            return datetime.now()

    def upsert_author(self, author_data: Dict, extract_date_sk: int) -> str:
        """Insert hoặc Update Author data"""
        try:
            with self.conn.cursor() as cursor:
                # Kiểm tra author đã tồn tại chưa
                cursor.execute(
                    "SELECT authorID, Name, avatar FROM Authors WHERE authorID = %s",
                    (author_data['authorID'],)
                )
                existing = cursor.fetchone()
                
                if existing:
                    # So sánh dữ liệu để quyết định update
                    if (existing['Name'] != author_data['Name'] or 
                        existing['avatar'] != author_data['avatar']):
                        
                        cursor.execute("""
                            UPDATE Authors 
                            SET Name = %s, avatar = %s, extract_date_sk = %s
                            WHERE authorID = %s
                        """, (
                            author_data['Name'],
                            author_data['avatar'], 
                            extract_date_sk,
                            author_data['authorID']
                        ))
                        return "UPDATE"
                    else:
                        return "NO_CHANGE"
                else:
                    # Insert new author
                    cursor.execute("""
                        INSERT INTO Authors (authorID, Name, avatar, extract_date_sk)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        author_data['authorID'],
                        author_data['Name'],
                        author_data['avatar'],
                        extract_date_sk
                    ))
                    return "INSERT"
                    
        except Exception as e:
            logger.error(f"Error upserting author {author_data['authorID']}: {e}")
            return "ERROR"

    def upsert_video(self, video_data: Dict) -> str:
        """Insert hoặc Update Video data"""
        try:
            with self.conn.cursor() as cursor:
                # Kiểm tra video đã tồn tại chưa
                cursor.execute("""
                    SELECT videoID, TextContent, Duration, WebVideoUrl 
                    FROM Videos WHERE videoID = %s
                """, (video_data['videoID'],))
                existing = cursor.fetchone()
                
                if existing:
                    # So sánh dữ liệu để quyết định update
                    if (existing['TextContent'] != video_data['TextContent'] or 
                        existing['Duration'] != video_data['Duration'] or
                        existing['WebVideoUrl'] != video_data['WebVideoUrl']):
                        
                        cursor.execute("""
                            UPDATE Videos 
                            SET TextContent = %s, Duration = %s, WebVideoUrl = %s, create_date_sk = %s
                            WHERE videoID = %s
                        """, (
                            video_data['TextContent'],
                            video_data['Duration'],
                            video_data['WebVideoUrl'],
                            video_data['create_date_sk'],
                            video_data['videoID']
                        ))
                        return "UPDATE"
                    else:
                        return "NO_CHANGE"
                else:
                    # Insert new video
                    cursor.execute("""
                        INSERT INTO Videos (videoID, authorID, TextContent, Duration, CreateTime, WebVideoUrl, create_date_sk)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        video_data['videoID'],
                        video_data['authorID'],
                        video_data['TextContent'],
                        video_data['Duration'],
                        video_data['CreateTime'],
                        video_data['WebVideoUrl'],
                        video_data['create_date_sk']
                    ))
                    return "INSERT"
                    
        except Exception as e:
            logger.error(f"Error upserting video {video_data['videoID']}: {e}")
            return "ERROR"

    def upsert_interaction(self, interaction_data: Dict) -> str:
        """Insert hoặc Update VideoInteractions data"""
        try:
            with self.conn.cursor() as cursor:
                # Kiểm tra interaction đã tồn tại chưa
                cursor.execute("""
                    SELECT videoID, DiggCount, PlayCount, ShareCount, CommentCount, CollectCount, interaction_date_sk
                    FROM VideoInteractions WHERE videoID = %s
                """, (interaction_data['videoID'],))
                existing = cursor.fetchone()
                
                today_sk = self.get_date_sk_for_today()
                
                if existing:
                    # Nếu đã có record, so sánh dữ liệu để quyết định update
                    if (existing['DiggCount'] != interaction_data['DiggCount'] or 
                        existing['PlayCount'] != interaction_data['PlayCount'] or
                        existing['ShareCount'] != interaction_data['ShareCount'] or
                        existing['CommentCount'] != interaction_data['CommentCount'] or
                        existing['CollectCount'] != interaction_data['CollectCount']):
                        
                        cursor.execute("""
                            UPDATE VideoInteractions 
                            SET DiggCount = %s, PlayCount = %s, ShareCount = %s, 
                                CommentCount = %s, CollectCount = %s, interaction_date_sk = %s
                            WHERE videoID = %s
                        """, (
                            interaction_data['DiggCount'],
                            interaction_data['PlayCount'],
                            interaction_data['ShareCount'],
                            interaction_data['CommentCount'],
                            interaction_data['CollectCount'],
                            today_sk,  # Always update with today's date_sk
                            interaction_data['videoID']
                        ))
                        return "UPDATE"
                    else:
                        # Nếu data giống nhau, vẫn update date_sk nếu cần
                        if existing['interaction_date_sk'] != today_sk:
                            cursor.execute("""
                                UPDATE VideoInteractions 
                                SET interaction_date_sk = %s
                                WHERE videoID = %s
                            """, (today_sk, interaction_data['videoID']))
                            return "DATE_UPDATE"
                        return "NO_CHANGE"
                else:
                    # Insert new interaction
                    cursor.execute("""
                        INSERT INTO VideoInteractions (videoID, DiggCount, PlayCount, ShareCount, CommentCount, CollectCount, interaction_date_sk)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        interaction_data['videoID'],
                        interaction_data['DiggCount'],
                        interaction_data['PlayCount'],
                        interaction_data['ShareCount'],
                        interaction_data['CommentCount'],
                        interaction_data['CollectCount'],
                        today_sk
                    ))
                    return "INSERT"
                    
        except Exception as e:
            logger.error(f"Error upserting interaction {interaction_data['videoID']}: {e}")
            return "ERROR"

    def log_operation(self, table_name: str, operation_counts: Dict, status: str = "success"):
        """Ghi log vào LoadLog table"""
        try:
            batch_id = f"etl_process_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            total_records = sum(operation_counts.values())
            
            # Tạo operation_type ngắn gọn
            ops = [k for k, v in operation_counts.items() if v > 0]
            operation_type = "ETL"
            if ops:
                operation_type = f"ETL_{','.join(ops[:2])}"  # Limit length
            
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO LoadLog (batch_id, table_name, operation_type, record_count, status, start_time, end_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    batch_id,
                    table_name,
                    operation_type,
                    total_records,
                    status,
                    datetime.now(),
                    datetime.now()
                ))
                
            logger.info(f"📝 Logged {table_name}: {operation_counts}")
            
        except Exception as e:
            logger.error(f"Error logging operation: {e}")

    def process_raw_json_data(self):
        """Main ETL process"""
        logger.info("🚀 Starting ETL Process...")
        
        try:
            extract_date_sk = self.get_date_sk_for_today()
            
            # Counters cho mỗi table
            author_counts = {"INSERT": 0, "UPDATE": 0, "NO_CHANGE": 0, "ERROR": 0}
            video_counts = {"INSERT": 0, "UPDATE": 0, "NO_CHANGE": 0, "ERROR": 0}
            interaction_counts = {"INSERT": 0, "UPDATE": 0, "NO_CHANGE": 0, "DATE_UPDATE": 0, "ERROR": 0}
            
            # Lấy tất cả raw json chưa xử lý
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, filename, content 
                    FROM RawJson 
                    WHERE load_status = 'PROCESSED'
                    ORDER BY id
                """)
                raw_records = cursor.fetchall()
                
            logger.info(f"📊 Processing {len(raw_records)} raw JSON records...")
            
            for record in raw_records:
                try:
                    # Parse JSON content if it's a string
                    json_content = record['content']
                    if isinstance(json_content, str):
                        json_content = json.loads(json_content)
                    
                    self.processed_records += 1
                    
                    # Extract & Upsert Author
                    author_data = self.extract_author_data(json_content, extract_date_sk)
                    if author_data:
                        result = self.upsert_author(author_data, extract_date_sk)
                        author_counts[result] += 1
                    
                    # Extract & Upsert Video
                    video_data = self.extract_video_data(json_content, extract_date_sk)
                    if video_data:
                        result = self.upsert_video(video_data)
                        video_counts[result] += 1
                    
                    # Extract & Upsert Interaction
                    interaction_data = self.extract_interaction_data(json_content, extract_date_sk)
                    if interaction_data:
                        result = self.upsert_interaction(interaction_data)
                        interaction_counts[result] += 1
                        
                    # Commit mỗi record để tránh lock lâu
                    self.conn.commit()
                    
                except Exception as e:
                    logger.error(f"Error processing record {record['id']}: {e}")
                    self.error_records += 1
                    self.conn.rollback()
                    continue
            
            # Log results
            self.log_operation("Authors", author_counts)
            self.log_operation("Videos", video_counts)
            self.log_operation("VideoInteractions", interaction_counts)
            
            # Summary
            logger.info(f"""
🎉 ETL Process Completed!
📊 Processed: {self.processed_records} records
👥 Authors - Insert: {author_counts['INSERT']}, Update: {author_counts['UPDATE']}, No Change: {author_counts['NO_CHANGE']}
🎬 Videos - Insert: {video_counts['INSERT']}, Update: {video_counts['UPDATE']}, No Change: {video_counts['NO_CHANGE']}  
📈 Interactions - Insert: {interaction_counts['INSERT']}, Update: {interaction_counts['UPDATE']}, No Change: {interaction_counts['NO_CHANGE']}
❌ Errors: {self.error_records}
            """)
            
        except Exception as e:
            logger.error(f"❌ ETL Process failed: {e}")
            raise

def run_etl_pipeline(connection):
    """Run the complete ETL pipeline"""
    processor = TikTokETLProcessor(connection)
    processor.process_raw_json_data()
    return processor

if __name__ == "__main__":
    # Example usage
    import pymysql
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    conn = pymysql.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        user=os.getenv('MYSQL_USER', 'user'),
        password=os.getenv('MYSQL_PASSWORD', 'password'),
        database='dbStaging',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        run_etl_pipeline(conn)
    finally:
        conn.close()