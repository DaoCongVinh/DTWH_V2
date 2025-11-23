"""
ETL Processor for TikTok Data with SCD Type 2 Logic
Extract từ RawJson -> Transform -> Load vào Authors, Videos, VideoInteractions
Implements Slowly Changing Dimension Type 2 with date-based surrogate keys
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
        self.stats = {
            'authors': {'INSERT': 0, 'UPDATE': 0, 'NO_CHANGE': 0, 'ERROR': 0},
            'videos': {'INSERT': 0, 'UPDATE': 0, 'NO_CHANGE': 0, 'ERROR': 0},
            'interactions': {'INSERT': 0, 'UPDATE': 0, 'NO_CHANGE': 0, 'ERROR': 0}
        }
        
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
            logger.error(f"Error extracting author data: {e}")
            return None

    def extract_video_data(self, json_content: dict, extract_date_sk: int) -> Optional[Dict]:
        """Extract thông tin Video từ JSON"""
        try:
            video_id = json_content.get('id')
            if not video_id:
                return None
                
            # Get author ID for foreign key
            author_meta = json_content.get('authorMeta', {})
            author_id = author_meta.get('id', 0) if author_meta else 0
            
            # Parse create time
            create_time = None
            create_time_raw = json_content.get('createTime', '')
            if create_time_raw:
                try:
                    if create_time_raw.isdigit():
                        create_time = datetime.fromtimestamp(int(create_time_raw))
                    else:
                        create_time = datetime.strptime(create_time_raw, '%Y-%m-%d %H:%M:%S')
                except:
                    create_time = None
            
            # create_date_sk lấy từ ngày hiện tại (ngày ETL chạy)
            return {
                'videoID': int(video_id),
                'authorID': int(author_id),
                'TextContent': json_content.get('text', '').strip(),
                'Duration': int(json_content.get('duration', 0)),
                'CreateTime': create_time,
                'WebVideoUrl': json_content.get('webVideoUrl', '').strip(),
                'create_date_sk': extract_date_sk  # Lấy từ ngày hiện tại
            }
        except Exception as e:
            logger.error(f"Error extracting video data: {e}")
            return None

    def extract_interaction_data(self, json_content: dict, extract_date_sk: int) -> Optional[Dict]:
        """Extract thông tin VideoInteractions từ JSON"""
        try:
            video_id = json_content.get('id')
            if not video_id:
                return None
            
            # interaction_date_sk lấy từ ngày hiện tại (ngày ETL chạy)
            return {
                'videoID': int(video_id),
                'DiggCount': int(json_content.get('diggCount', 0)),
                'PlayCount': int(json_content.get('playCount', 0)),
                'ShareCount': int(json_content.get('shareCount', 0)),
                'CommentCount': int(json_content.get('commentCount', 0)),
                'CollectCount': int(json_content.get('collectCount', 0)),
                'interaction_date_sk': extract_date_sk  # Lấy từ ngày hiện tại
            }
        except Exception as e:
            logger.error(f"Error extracting interaction data: {e}")
            return None

    def upsert_author_scd2(self, author_data: Dict) -> str:
        """Insert hoặc Update Authors data với SCD Type 2 logic"""
        try:
            with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
                today_sk = self.get_date_sk_for_today()
                today_date = date.today()
                
                # Tìm record hiện tại của author (latest by date)
                cursor.execute("""
                    SELECT a.authorID, a.Name, a.avatar, a.extract_date_sk, d.full_date
                    FROM Authors a
                    LEFT JOIN DateDim d ON a.extract_date_sk = d.date_sk
                    WHERE a.authorID = %s 
                    ORDER BY a.extract_date_sk DESC 
                    LIMIT 1
                """, (author_data['authorID'],))
                existing = cursor.fetchone()
                
                if existing and existing['full_date']:
                    existing_date = datetime.strptime(existing['full_date'], '%Y-%m-%d').date()
                    
                    # So sánh dữ liệu có thay đổi không
                    data_changed = (
                        existing['Name'] != author_data['Name'] or 
                        existing['avatar'] != author_data['avatar']
                    )
                    
                    if existing_date == today_date:
                        # Cùng ngày: UPDATE record hiện tại, giữ nguyên surrogate key
                        if data_changed:
                            cursor.execute("""
                                UPDATE Authors 
                                SET Name = %s, avatar = %s
                                WHERE authorID = %s AND extract_date_sk = %s
                            """, (
                                author_data['Name'],
                                author_data['avatar'],
                                author_data['authorID'],
                                existing['extract_date_sk']
                            ))
                            return "UPDATE"
                        else:
                            return "NO_CHANGE"
                    else:
                        # Khác ngày: INSERT record mới với extract_date_sk mới nếu có thay đổi
                        if data_changed:
                            cursor.execute("""
                                INSERT INTO Authors (authorID, Name, avatar, extract_date_sk)
                                VALUES (%s, %s, %s, %s)
                            """, (
                                author_data['authorID'],  # Giữ nguyên authorID (business key)
                                author_data['Name'],
                                author_data['avatar'],
                                today_sk  # extract_date_sk mới tạo record mới với composite key (authorID, extract_date_sk)
                            ))
                            return "INSERT"
                        else:
                            return "NO_CHANGE"
                else:
                    # Chưa có record nào: INSERT record đầu tiên
                    cursor.execute("""
                        INSERT INTO Authors (authorID, Name, avatar, extract_date_sk)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        author_data['authorID'],
                        author_data['Name'],
                        author_data['avatar'],
                        today_sk
                    ))
                    return "INSERT"
                    
        except Exception as e:
            logger.error(f"Error upserting author {author_data['authorID']}: {e}")
            return "ERROR"

    def upsert_video_scd2(self, video_data: Dict) -> str:
        """Insert hoặc Update Videos data với SCD Type 2 logic"""
        try:
            with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
                today_sk = self.get_date_sk_for_today()
                today_date = date.today()
                
                # Tìm record hiện tại của video (videos chỉ có 1 record per videoID)
                cursor.execute("""
                    SELECT v.videoID, v.authorID, v.TextContent, v.Duration, v.CreateTime, v.WebVideoUrl, 
                           v.create_date_sk, d.full_date
                    FROM Videos v
                    LEFT JOIN DateDim d ON v.create_date_sk = d.date_sk
                    WHERE v.videoID = %s 
                    LIMIT 1
                """, (video_data['videoID'],))
                existing = cursor.fetchone()
                
                if existing:
                    # So sánh dữ liệu có thay đổi không
                    data_changed = (
                        existing['authorID'] != video_data['authorID'] or 
                        existing['TextContent'] != video_data['TextContent'] or
                        existing['Duration'] != video_data['Duration'] or
                        str(existing['CreateTime']) != str(video_data['CreateTime']) or
                        existing['WebVideoUrl'] != video_data['WebVideoUrl']
                    )
                    
                    if data_changed:
                        # UPDATE record hiện tại
                        cursor.execute("""
                            UPDATE Videos 
                            SET authorID = %s, TextContent = %s, Duration = %s, 
                                CreateTime = %s, WebVideoUrl = %s,
                                create_date_sk = %s
                            WHERE videoID = %s
                        """, (
                            video_data['authorID'],
                            video_data['TextContent'],
                            video_data['Duration'],
                            video_data['CreateTime'],
                            video_data['WebVideoUrl'],
                            today_sk,
                            video_data['videoID']
                        ))
                        return "UPDATE"
                    else:
                        return "NO_CHANGE"
                else:
                    # Chưa có record nào: INSERT record đầu tiên
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
                        today_sk
                    ))
                    return "INSERT"
                    
        except Exception as e:
            logger.error(f"Error upserting video {video_data['videoID']}: {e}")
            return "ERROR"

    def upsert_interaction_scd2(self, interaction_data: Dict) -> str:
        """Insert hoặc Update VideoInteractions data với SCD Type 2 logic"""
        try:
            with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
                today_sk = self.get_date_sk_for_today()
                
                # Tìm record hiện tại của interaction (videoInteractions chỉ có 1 record per videoID)
                cursor.execute("""
                    SELECT vi.videoID, vi.DiggCount, vi.PlayCount, vi.ShareCount, vi.CommentCount, vi.CollectCount, 
                           vi.interaction_date_sk
                    FROM VideoInteractions vi
                    WHERE vi.videoID = %s 
                    LIMIT 1
                """, (interaction_data['videoID'],))
                existing = cursor.fetchone()
                
                if existing:
                    # So sánh dữ liệu có thay đổi không
                    data_changed = (
                        existing['DiggCount'] != interaction_data['DiggCount'] or 
                        existing['PlayCount'] != interaction_data['PlayCount'] or
                        existing['ShareCount'] != interaction_data['ShareCount'] or
                        existing['CommentCount'] != interaction_data['CommentCount'] or
                        existing['CollectCount'] != interaction_data['CollectCount']
                    )
                    
                    if data_changed:
                        # UPDATE record hiện tại
                        cursor.execute("""
                            UPDATE VideoInteractions 
                            SET DiggCount = %s, PlayCount = %s, ShareCount = %s, 
                                CommentCount = %s, CollectCount = %s,
                                interaction_date_sk = %s
                            WHERE videoID = %s
                        """, (
                            interaction_data['DiggCount'],
                            interaction_data['PlayCount'],
                            interaction_data['ShareCount'],
                            interaction_data['CommentCount'],
                            interaction_data['CollectCount'],
                            today_sk,
                            interaction_data['videoID']
                        ))
                        return "UPDATE"
                    else:
                        return "NO_CHANGE"
                else:
                    # Chưa có record nào: INSERT record đầu tiên
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

    def process_json_record(self, filename: str, json_content: dict) -> dict:
        """Process một JSON record và extract data vào các bảng"""
        try:
            today_sk = self.get_date_sk_for_today()
            results = {'author': None, 'video': None, 'interaction': None}
            
            # Extract và upsert Author
            author_data = self.extract_author_data(json_content, today_sk)
            if author_data:
                author_result = self.upsert_author_scd2(author_data)
                self.stats['authors'][author_result] += 1
                results['author'] = author_result
            
            # Extract và upsert Video
            video_data = self.extract_video_data(json_content, today_sk)
            if video_data:
                video_result = self.upsert_video_scd2(video_data)
                self.stats['videos'][video_result] += 1
                results['video'] = video_result
            
            # Extract và upsert VideoInteraction
            interaction_data = self.extract_interaction_data(json_content, today_sk)
            if interaction_data:
                interaction_result = self.upsert_interaction_scd2(interaction_data)
                self.stats['interactions'][interaction_result] += 1
                results['interaction'] = interaction_result
            
            self.processed_records += 1
            return results
            
        except Exception as e:
            logger.error(f"Error processing JSON record from {filename}: {e}")
            return {'error': str(e)}

    def process_all_raw_json(self):
        """Process tất cả records từ RawJson table"""
        try:
            with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
                # Lấy tất cả raw JSON records chưa processed
                cursor.execute("""
                    SELECT id, filename, content 
                    FROM RawJson 
                    WHERE load_status = 'success'
                    ORDER BY loaded_at ASC
                """)
                raw_records = cursor.fetchall()
                
                # Chỉ log khi có records để process
                if len(raw_records) == 0:
                    return  # Không có records, không cần log gì cả
                
                logger.info(f"🚀 Starting ETL Process... Processing {len(raw_records)} raw JSON records...")
                
                for record in raw_records:
                    try:
                        json_content = record['content']
                        if isinstance(json_content, str):
                            json_content = json.loads(json_content)
                        
                        result = self.process_json_record(record['filename'], json_content)
                        
                        # Update load_status to 'processed' after successful processing
                        if 'error' not in result:
                            cursor.execute(
                                "UPDATE RawJson SET load_status = 'processed' WHERE id = %s",
                                (record['id'],)
                            )
                        
                        if self.processed_records % 50 == 0:
                            logger.info(f"📈 Processed {self.processed_records} records...")
                            
                    except Exception as e:
                        logger.error(f"Error processing record {record['id']}: {e}")
                        self.stats['authors']['ERROR'] += 1
                        continue
                
                # Commit all changes
                self.conn.commit()
                
                # Log final stats
                self.log_final_stats()
                
        except Exception as e:
            logger.error(f"Error in ETL process: {e}")
            self.conn.rollback()
            raise

    def log_final_stats(self):
        """Log thống kê cuối cùng - chỉ log khi có records được process"""
        if self.processed_records == 0:
            return  # Không có records, không log
        
        total_errors = self.stats['authors']['ERROR'] + self.stats['videos']['ERROR'] + self.stats['interactions']['ERROR']
        
        # Log tóm tắt ngắn gọn
        logger.info(f"✅ ETL Completed: {self.processed_records} records | "
                   f"Authors: +{self.stats['authors']['INSERT']} ~{self.stats['authors']['UPDATE']} | "
                   f"Videos: +{self.stats['videos']['INSERT']} ~{self.stats['videos']['UPDATE']} | "
                   f"Interactions: +{self.stats['interactions']['INSERT']} ~{self.stats['interactions']['UPDATE']} | "
                   f"Errors: {total_errors}")


def run_etl_pipeline(connection):
    """Main ETL pipeline runner"""
    try:
        processor = TikTokETLProcessor(connection)
        processor.process_all_raw_json()
        return True
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        return False