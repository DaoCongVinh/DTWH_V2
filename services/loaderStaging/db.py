"""
Database Helper Module
Handles all database operations: connections, batch fetch, upsert, logging
"""

import json
import logging
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
import mysql.connector
from mysql.connector import Error, MySQLConnection
from contextlib import contextmanager

import config

logger = logging.getLogger(__name__)

# ============================================================================
# Database Connection Manager
# ============================================================================

class DatabaseConnection:
    """
    Class DatabaseConnection: Quản lý kết nối MySQL database
    
    Trách nhiệm:
    - Quản lý connection lifecycle (connect, disconnect, check status)
    - Cung cấp cursor cho các operations
    - Handle connection pooling và errors
    - Context manager cho safe cursor usage
    """
    
    def __init__(self):
        """Khởi tạo connection object (chưa kết nối)"""
        self.connection: Optional[MySQLConnection] = None
    
    def connect(self) -> MySQLConnection:
        """
        Thiết lập kết nối đến MySQL database
        
        Đọc thông tin kết nối từ config:
        - MYSQL_HOST: Hostname của MySQL server
        - MYSQL_PORT: Port (mặc định 3306)
        - MYSQL_USER: Username
        - MYSQL_PASSWORD: Password
        - MYSQL_DATABASE: Tên database (dbStaging)
        
        Returns:
            MySQLConnection: Connection object đang active
            
        Raises:
            Error: Nếu kết nối thất bại (wrong credentials, network issue, etc.)
        """
        try:
            self.connection = mysql.connector.connect(
                host=config.MYSQL_HOST,
                port=config.MYSQL_PORT,
                user=config.MYSQL_USER,
                password=config.MYSQL_PASSWORD,
                database=config.MYSQL_DATABASE,
                charset=config.DB_CONFIG["charset"],
                autocommit=config.DB_CONFIG["autocommit"]
            )
            logger.info(f"Connected to database: {config.MYSQL_DATABASE}")
            return self.connection
        except Error as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def disconnect(self) -> None:
        """
        Đóng kết nối database
        
        Gọi method này khi:
        - Hoàn tất toàn bộ operations
        - Xảy ra lỗi và cần cleanup
        - Shutdown service
        """
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")
    
    def is_connected(self) -> bool:
        """
        Kiểm tra connection còn active không
        
        Returns:
            bool: True nếu connection đang active
        """
        return self.connection and self.connection.is_connected()
    
    @contextmanager
    def get_cursor(self, buffered: bool = False):
        """
        Context manager để lấy database cursor một cách an toàn
        
        Sử dụng:
        ```python
        with db_conn.get_cursor() as cursor:
            cursor.execute("SELECT ...")
            results = cursor.fetchall()
        # Cursor tự động đóng sau khi exit context
        ```
        
        Args:
            buffered: True = buffered cursor (cho nhiều queries)
                     False = unbuffered cursor (tiết kiệm memory)
            
        Yields:
            CMySQLCursor: Database cursor để execute queries
        """
        if not self.is_connected():
            self.connect()
        
        cursor = self.connection.cursor(buffered=buffered)
        try:
            yield cursor
        finally:
            cursor.close()  # Luôn đóng cursor khi xong

# ============================================================================
# Batch Fetch Operations
# ============================================================================

class BatchFetcher:
    """
    Class BatchFetcher: Batch fetch operations để tối ưu ETL
    
    Tại sao cần Batch Fetch?
    - Thay vì query DB cho MỐI record (10,000 queries) → Chỉ 3 queries
    - Load tất cả IDs vào memory → Check existence với O(1)
    - Tăng tốc độ gấp 100-1000 lần
    - Giảm load cho database server
    
    Các operations:
    - fetch_all_authors(): Lấy tất cả author IDs
    - fetch_all_videos(): Lấy tất cả video IDs
    - fetch_all_interactions(): Lấy video IDs có interactions
    - get_today_date_sk(): Lấy date_sk của hôm nay
    """
    
    def __init__(self, db_conn: DatabaseConnection):
        """Khởi tạo với database connection"""
        self.db_conn = db_conn
    
    def fetch_all_authors(self) -> Set[str]:
        """
        Fetch tất cả author IDs đã tồn tại trong DB
        
        Query: SELECT DISTINCT author_id FROM Authors
        
        Mục đích:
        - Cache tất cả author IDs trong memory
        - Check author exists bằng: author_id in existing_authors (O(1))
        - Không cần query DB cho mỗi record
        
        Returns:
            Set[str]: Set chứa tất cả author IDs
        """
        try:
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(config.Queries.GET_ALL_AUTHORS)
                results = cursor.fetchall()
                # Convert list of tuples thành Set of IDs
                author_ids = {row[0] for row in results}
                logger.info(f"Fetched {len(author_ids)} existing authors")
                return author_ids
        except Error as e:
            logger.error(f"Error fetching authors: {e}")
            return set()  # Return empty set nếu lỗi
    
    def fetch_all_videos(self) -> Set[str]:
        """
        Fetch tất cả video IDs đã tồn tại trong DB
        
        Query: SELECT DISTINCT video_id FROM Videos
        
        Returns:
            Set[str]: Set chứa tất cả video IDs
        """
        try:
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(config.Queries.GET_ALL_VIDEOS)
                results = cursor.fetchall()
                video_ids = {row[0] for row in results}
                logger.info(f"Fetched {len(video_ids)} existing videos")
                return video_ids
        except Error as e:
            logger.error(f"Error fetching videos: {e}")
            return set()
    
    def fetch_all_interactions(self) -> Set[str]:
        """
        Fetch all video IDs with existing interactions
        
        Returns:
            Set[str]: Set of video IDs with interactions
        """
        try:
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(config.Queries.GET_ALL_INTERACTIONS)
                results = cursor.fetchall()
                video_ids = {row[0] for row in results}
                logger.info(f"Fetched {len(video_ids)} videos with interactions")
                return video_ids
        except Error as e:
            logger.error(f"Error fetching interactions: {e}")
            return set()
    
    def fetch_all(self) -> Tuple[Set[str], Set[str], Set[str]]:
        """
        Batch fetch tất cả data trong 3 queries song song
        
        Thực hiện:
        1. Fetch all author IDs
        2. Fetch all video IDs
        3. Fetch all video IDs with interactions
        
        Lợi ích:
        - Chỉ 3 queries thay vì hàng nghìn queries
        - Tải data 1 lần vào memory
        - Lookup cực nhanh với Set.in (O(1))
        
        Returns:
            Tuple chứa 3 Sets:
            - Set[str]: Author IDs
            - Set[str]: Video IDs
            - Set[str]: Video IDs with interactions
        """
        logger.info("Starting batch fetch...")
        authors = self.fetch_all_authors()
        videos = self.fetch_all_videos()
        interactions = self.fetch_all_interactions()
        logger.info(f"Batch fetch complete: {len(authors)} authors, {len(videos)} videos, {len(interactions)} interactions")
        return authors, videos, interactions
    
    def get_today_date_sk(self) -> Optional[int]:
        """
        Get today's date surrogate key
        
        Returns:
            Optional[int]: Today's date_sk or None if not found
        """
        try:
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(config.Queries.GET_TODAY_DATE_SK)
                result = cursor.fetchone()
                if result:
                    date_sk = result[0]
                    logger.info(f"Today's date_sk: {date_sk}")
                    return date_sk
                logger.warning("Today's date not found in DateDim")
                return None
        except Error as e:
            logger.error(f"Error getting today's date_sk: {e}")
            return None

# ============================================================================
# Raw JSON Operations
# ============================================================================

class RawJsonManager:
    """
    Class RawJsonManager: Quản lý bảng RawJson
    
    Bảng RawJson lưu giữ:
    - Toàn bộ JSON gốc từ crawler
    - Filename và timestamp
    - Load status (SUCCESS/FAILED)
    - Error message nếu có
    
    Mục đích:
    - Audit trail - Trace back được dữ liệu gốc
    - Debugging - Xem lại JSON khi có issue
    - Reprocess - Load lại nếu cần
    - Compliance - Giữ lại raw data theo quy định
    
    Logic mới:
    - So sánh JSON mới với JSON cũ trong DB
    - Chỉ insert items MỚI (chưa có video_id trong DB)
    - Xóa items CŨ sau khi load thành công
    """
    
    def __init__(self, db_conn: DatabaseConnection):
        """Khởi tạo với database connection"""
        self.db_conn = db_conn
    
    def fetch_existing_video_ids(self) -> Set[str]:
        """
        Lấy tất cả video IDs đã có trong bảng RawJson
        
        Mục đích:
        - So sánh với JSON mới để tìm items mới
        - Tránh duplicate data
        - Chỉ load data mới vào DB
        
        Returns:
            Set[str]: Set chứa tất cả video IDs trong RawJson
        """
        try:
            video_ids = set()
            with self.db_conn.get_cursor() as cursor:
                # Lấy tất cả raw JSON có status SUCCESS
                cursor.execute("""
                    SELECT content FROM RawJson 
                    WHERE load_status = 'SUCCESS' AND content != ''
                """)
                results = cursor.fetchall()
                
                # Parse JSON và extract video IDs
                for row in results:
                    try:
                        content = row[0]
                        if content:
                            json_data = json.loads(content)
                            # JSON có thể là array hoặc single object
                            if isinstance(json_data, list):
                                for item in json_data:
                                    if 'id' in item:
                                        video_ids.add(item['id'])
                            elif isinstance(json_data, dict) and 'id' in json_data:
                                video_ids.add(json_data['id'])
                    except json.JSONDecodeError:
                        continue  # Skip invalid JSON
                
                logger.info(f"Fetched {len(video_ids)} existing video IDs from RawJson")
                return video_ids
        except Error as e:
            logger.error(f"Error fetching existing video IDs: {e}")
            return set()
    
    def delete_old_raw_json(self, video_ids_to_delete: Set[str]) -> bool:
        """
        Xóa các records cũ trong RawJson có chứa video IDs cần xóa
        
        Logic:
        - Đọc từng record trong RawJson
        - Parse JSON và check video IDs
        - Xóa record nếu chứa video IDs cũ
        
        Args:
            video_ids_to_delete: Set các video IDs cần xóa
            
        Returns:
            bool: True nếu xóa thành công
        """
        if not video_ids_to_delete:
            logger.info("No old video IDs to delete")
            return True
        
        try:
            deleted_count = 0
            with self.db_conn.get_cursor() as cursor:
                # Lấy tất cả records
                cursor.execute("""
                    SELECT raw_json_id, content FROM RawJson 
                    WHERE load_status = 'SUCCESS' AND content != ''
                """)
                results = cursor.fetchall()
                
                records_to_delete = []
                
                # Check từng record
                for row in results:
                    raw_json_id = row[0]
                    content = row[1]
                    try:
                        json_data = json.loads(content)
                        should_delete = False
                        
                        # Check nếu JSON chứa video IDs cần xóa
                        if isinstance(json_data, list):
                            for item in json_data:
                                if 'id' in item and item['id'] in video_ids_to_delete:
                                    should_delete = True
                                    break
                        elif isinstance(json_data, dict) and 'id' in json_data:
                            if json_data['id'] in video_ids_to_delete:
                                should_delete = True
                        
                        if should_delete:
                            records_to_delete.append(raw_json_id)
                    except json.JSONDecodeError:
                        continue
                
                # Xóa records
                if records_to_delete:
                    placeholders = ','.join(['%s'] * len(records_to_delete))
                    delete_query = f"DELETE FROM RawJson WHERE raw_json_id IN ({placeholders})"
                    cursor.execute(delete_query, records_to_delete)
                    deleted_count = cursor.rowcount
                    self.db_conn.connection.commit()
                    logger.info(f"Deleted {deleted_count} old records from RawJson")
                else:
                    logger.info("No records to delete")
                
                return True
        except Error as e:
            logger.error(f"Error deleting old raw JSON: {e}")
            self.db_conn.connection.rollback()
            return False
    
    def insert_raw_json(
        self,
        content: str,
        filename: str,
        status: str = config.LOAD_STATUS_SUCCESS,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Insert raw JSON record (CHỈ ITEMS MỚI)
        
        Logic mới:
        1. Lấy tất cả video IDs đã có trong RawJson
        2. Parse JSON mới và filter chỉ lấy items mới
        3. Insert items mới vào RawJson
        4. Xóa items cũ khỏi RawJson
        
        Args:
            content: Full JSON content
            filename: Source file name
            status: Load status (SUCCESS/FAILED)
            error_message: Error details if failed
            
        Returns:
            bool: True if successful
        """
        try:
            # Nếu status FAILED, insert nguyên xi không filter
            if status == config.LOAD_STATUS_FAILED:
                with self.db_conn.get_cursor() as cursor:
                    cursor.execute(
                        config.Queries.INSERT_RAW_JSON,
                        (content, filename, status, error_message)
                    )
                    self.db_conn.connection.commit()
                    logger.info(f"Inserted failed raw JSON record: {filename}")
                    return True
            
            # === LOGIC MỚI: CHỈ INSERT ITEMS MỚI ===
            
            # 1. Lấy video IDs đã có trong DB
            existing_video_ids = self.fetch_existing_video_ids()
            logger.info(f"Found {len(existing_video_ids)} existing video IDs in RawJson")
            
            # 2. Parse JSON mới
            try:
                json_data = json.loads(content)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON content: {e}")
                return False
            
            # 3. Filter chỉ lấy items MỚI
            new_items = []
            old_video_ids = set()
            
            if isinstance(json_data, list):
                for item in json_data:
                    video_id = item.get('id')
                    if video_id:
                        if video_id not in existing_video_ids:
                            new_items.append(item)  # Item MỚI
                        else:
                            old_video_ids.add(video_id)  # Item CŨ - đánh dấu để xóa
            elif isinstance(json_data, dict):
                video_id = json_data.get('id')
                if video_id:
                    if video_id not in existing_video_ids:
                        new_items.append(json_data)
                    else:
                        old_video_ids.add(video_id)
            
            logger.info(f"Found {len(new_items)} NEW items, {len(old_video_ids)} OLD items")
            
            # 4. Nếu không có items mới, return
            if not new_items:
                logger.info("No new items to insert")
                # Vẫn xóa items cũ nếu có
                if old_video_ids:
                    self.delete_old_raw_json(old_video_ids)
                return True
            
            # 5. Insert items MỚI
            new_content = json.dumps(new_items, ensure_ascii=False)
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(
                    config.Queries.INSERT_RAW_JSON,
                    (new_content, filename, status, error_message)
                )
                self.db_conn.connection.commit()
                logger.info(f"Inserted {len(new_items)} NEW items into RawJson: {filename}")
            
            # 6. Xóa items CŨ
            if old_video_ids:
                self.delete_old_raw_json(old_video_ids)
                logger.info(f"Deleted {len(old_video_ids)} OLD video IDs from RawJson")
            
            return True
            
        except Error as e:
            logger.error(f"Error inserting raw JSON: {e}")
            self.db_conn.connection.rollback()
            return False

# ============================================================================
# Upsert Operations (SCD Type 2)
# ============================================================================

class UpsertManager:
    """
    Class UpsertManager: Quản lý upsert operations với SCD Type 2 logic
    
    SCD Type 2 (Slowly Changing Dimension):
    - Giữ track lịch sử thay đổi của dữ liệu
    - Mỗi record có is_current flag
    - Khi data thay đổi: UPDATE record hiện tại
    
    Logic upsert cho mỗi record:
    1. Check ID có tồn tại trong existing_* Set không
    2. Nếu CHƯA tồn tại → INSERT record mới
    3. Nếu ĐÃ tồn tại:
       a. So sánh data có thay đổi không
       b. Nếu thay đổi → UPDATE record
       c. Nếu không đổi → SKIP (tiết kiệm query)
    
    Returns cho mỗi operation:
    - (True, "INSERT"): Insert thành công
    - (True, "UPDATE"): Update thành công
    - (True, "SKIP"): Không cần update
    - (False, "ERROR"): Có lỗi xảy ra
    """
    
    def __init__(self, db_conn: DatabaseConnection):
        """Khởi tạo với database connection"""
        self.db_conn = db_conn
    
    def upsert_author(self,
        author_id: str,
        author_name: str,
        avatar: str,
        date_sk: int,
        existing_authors: Set[str],
        existing_data: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:  # (success, action: INSERT/UPDATE/SKIP)
        """
        Upsert author record với SCD Type 2 logic
        
        Flow diagram:
        ```
        author_id in existing_authors?
                │
           ┌────┼────┐
           │         │
          NO        YES
           │         │
           │    Check data changed?
           │         │
           │    ┌────┼────┐
           │    │         │
           │   YES        NO
           │    │         │
        INSERT UPDATE    SKIP
        ```
        
        Args:
            author_id: Author ID (PK)
            author_name: Tên author
            avatar: Avatar URL
            date_sk: Date surrogate key (FK to DateDim)
            existing_authors: Set các author_id đã có
            existing_data: Data hiện tại (cho compare) - optional
            
        Returns:
            Tuple (success: bool, action: str)
            - (True, "INSERT"): Insert author mới
            - (True, "UPDATE"): Update author cũ
            - (True, "SKIP"): Không cần update
            - (False, "ERROR"): Có lỗi
        """
        try:
            if author_id not in existing_authors:
                # ===== CASE 1: INSERT NEW AUTHOR =====
                with self.db_conn.get_cursor() as cursor:
                    cursor.execute(
                        config.Queries.INSERT_AUTHOR,
                        (author_id, author_name, avatar, date_sk)
                    )
                self.db_conn.connection.commit()
                return True, "INSERT"
            else:
                # ===== CASE 2 & 3: AUTHOR ĐÃ TỒN TẠI =====
                # Check data có thay đổi không
                if existing_data and (
                    existing_data.get("author_name") == author_name and
                    existing_data.get("avatar") == avatar
                ):
                    # CASE 3: Data không đổi → SKIP
                    return True, "SKIP"
                else:
                    # CASE 2: Data thay đổi → UPDATE
                    with self.db_conn.get_cursor() as cursor:
                        cursor.execute(
                            config.Queries.UPDATE_AUTHOR,
                            (author_name, avatar, author_id, date_sk)
                        )
                    self.db_conn.connection.commit()
                    return True, "UPDATE"
        except Error as e:
            logger.error(f"Error upserting author {author_id}: {e}")
            self.db_conn.connection.rollback()
            return False, "ERROR"
    
    def upsert_video(
        self,
        video_id: str,
        author_id: str,
        text_content: str,
        duration: int,
        create_time: str,
        web_video_url: str,
        date_sk: int,
        existing_videos: Set[str],
        existing_data: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """
        Upsert video record (SCD Type 2)
        
        Args:
            video_id: Video ID
            author_id: Author ID (FK)
            text_content: Video caption
            duration: Duration in seconds
            create_time: Creation timestamp
            web_video_url: TikTok URL
            date_sk: Date surrogate key
            existing_videos: Set of existing video IDs
            existing_data: Existing video data for comparison
            
        Returns:
            Tuple of (success, action_taken)
        """
        try:
            if video_id not in existing_videos:
                # INSERT new video
                with self.db_conn.get_cursor() as cursor:
                    cursor.execute(
                        config.Queries.INSERT_VIDEO,
                        (video_id, author_id, text_content, duration, create_time, web_video_url, date_sk)
                    )
                self.db_conn.connection.commit()
                return True, "INSERT"
            else:
                # Video exists - check if data changed
                if existing_data and (
                    existing_data.get("text_content") == text_content and
                    existing_data.get("duration") == duration and
                    existing_data.get("create_time") == create_time
                ):
                    # No change - SKIP
                    return True, "SKIP"
                else:
                    # Data changed - UPDATE
                    with self.db_conn.get_cursor() as cursor:
                        cursor.execute(
                            config.Queries.UPDATE_VIDEO,
                            (text_content, duration, create_time, web_video_url, video_id, date_sk)
                        )
                    self.db_conn.connection.commit()
                    return True, "UPDATE"
        except Error as e:
            logger.error(f"Error upserting video {video_id}: {e}")
            self.db_conn.connection.rollback()
            return False, "ERROR"
    
    def upsert_interaction(
        self,
        video_id: str,
        digg_count: int,
        play_count: int,
        share_count: int,
        comment_count: int,
        collect_count: int,
        date_sk: int,
        existing_interactions: Set[str],
        existing_data: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """
        Upsert video interaction record với SCD Type 2 logic
        
        Đặc điểm:
        - Interactions LUÔN LUÔN THAY ĐỔI (views, likes tăng liên tục)
        - Cần update thường xuyên để tracking metrics
        - SKIP chỉ khi TẤT CẢ 5 counts đều giống hệt (rất hiếm)
        
        5 metrics được track:
        1. digg_count: Số likes
        2. play_count: Số views
        3. share_count: Số shares
        4. comment_count: Số comments
        5. collect_count: Số saves
        
        Args:
            video_id: Video ID (FK to Videos)
            digg_count: Số like
            play_count: Số view
            share_count: Số share
            comment_count: Số comment
            collect_count: Số save
            date_sk: Date surrogate key
            existing_interactions: Set các video_id đã có interaction
            existing_data: Data hiện tại (cho compare)
            
        Returns:
            Tuple (success: bool, action: str)
        """
        try:
            if video_id not in existing_interactions:
                # ===== INSERT NEW INTERACTION =====
                with self.db_conn.get_cursor() as cursor:
                    cursor.execute(
                        config.Queries.INSERT_INTERACTION,
                        (video_id, digg_count, play_count, share_count, comment_count, collect_count, date_sk)
                    )
                self.db_conn.connection.commit()
                return True, "INSERT"
            else:
                # ===== INTERACTION ĐÃ TỐN TẠI - CHECK COUNTS CHANGED =====
                if existing_data and (
                    existing_data.get("digg_count") == digg_count and
                    existing_data.get("play_count") == play_count and
                    existing_data.get("share_count") == share_count and
                    existing_data.get("comment_count") == comment_count and
                    existing_data.get("collect_count") == collect_count
                ):
                    # Tất cả counts giống hệt → SKIP (rất hiếm)
                    return True, "SKIP"
                else:
                    # Ít nhất 1 count thay đổi → UPDATE
                    with self.db_conn.get_cursor() as cursor:
                        cursor.execute(
                            config.Queries.UPDATE_INTERACTION,
                            (digg_count, play_count, share_count, comment_count, collect_count, video_id, date_sk)
                        )
                    self.db_conn.connection.commit()
                    return True, "UPDATE"
        except Error as e:
            logger.error(f"Error upserting interaction {video_id}: {e}")
            self.db_conn.connection.rollback()
            return False, "ERROR"

# ============================================================================
# Load Log Manager
# ============================================================================

class LoadLogManager:
    """
    Class LoadLogManager: Quản lý bảng LoadLog
    
    Bảng LoadLog ghi lại:
    - Mỗi lần load vào staging table
    - Statistics: inserted, updated, skipped, failed counts
    - Thời gian xử lý (start, end, duration)
    - Status: SUCCESS/FAILED/PARTIAL
    - Source filename và batch_id
    
    Mục đích:
    - Monitoring - Theo dõi quá trình load
    - Troubleshooting - Debug khi có issue
    - Reporting - Báo cáo số liệu load
    - Audit - Compliance với quy định
    """
    
    def __init__(self, db_conn: DatabaseConnection):
        """Khởi tạo với database connection"""
        self.db_conn = db_conn
    
    def log_load(
        self,
        batch_id: str,
        table_name: str,
        record_count: int,
        inserted_count: int,
        updated_count: int,
        skipped_count: int,
        status: str,
        start_time: datetime,
        end_time: datetime,
        source_filename: str,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Insert load log record vào bảng LoadLog
        
        Gọi method này sau khi hoàn tất load một staging table
        
        Ví dụ log record:
        - batch_id: "LOAD_20231129_123456"
        - table_name: "Authors"
        - record_count: 100 (tổng số records xử lý)
        - inserted_count: 20 (records mới)
        - updated_count: 15 (records cập nhật)
        - skipped_count: 65 (records không đổi)
        - status: "SUCCESS" hoặc "FAILED" hoặc "PARTIAL"
        - duration_seconds: 16.5
        - source_filename: "device-unknown_run_23112025T044106Z.json"
        
        Args:
            batch_id: Batch identifier (LOAD_YYYYMMDD_HHMMSS)
            table_name: Tên bảng đích (Authors/Videos/VideoInteractions)
            record_count: Tổng số records xử lý
            inserted_count: Số records INSERT
            updated_count: Số records UPDATE
            skipped_count: Số records SKIP
            status: LOAD_STATUS_SUCCESS/FAILED/PARTIAL
            start_time: Thời điểm bắt đầu
            end_time: Thời điểm kết thúc
            source_filename: Tên file JSON gốc
            error_message: Thông tin lỗi nếu có
            
        Returns:
            bool: True nếu log thành công
        """
        try:
            # Tính duration từ start và end time
            duration_seconds = (end_time - start_time).total_seconds()
            
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(
                    config.Queries.INSERT_LOAD_LOG,
                    (
                        batch_id, table_name, record_count,
                        inserted_count, updated_count, skipped_count,
                        status, start_time, end_time,
                        duration_seconds, source_filename, error_message
                    )
                )
            self.db_conn.connection.commit()
            logger.info(
                f"Logged: {table_name} - {inserted_count} inserted, {updated_count} updated, "
                f"{skipped_count} skipped in {duration_seconds:.2f}s"
            )
            return True
        except Error as e:
            logger.error(f"Error logging load: {e}")
            self.db_conn.connection.rollback()
            return False

# ============================================================================
# DateDim Manager
# ============================================================================

class DateDimManager:
    """
    Class DateDimManager: Quản lý bảng DateDim (Date Dimension)
    
    Bảng DateDim chứa:
    - Tất cả ngày từ 2005 đến tương lai
    - 18 cột thông tin về mỗi ngày
    - date_sk (PK) - Surrogate key dạng số
    - full_date - Ngày đầy đủ (YYYY-MM-DD)
    - Các thông tin khác: day_of_week, month, year, week, holiday...
    
    Mục đích:
    - Chuẩn hóa date dimension trong data warehouse
    - Cho phép join với các fact tables qua date_sk
    - Hỗ trợ time-based analysis (weekly, monthly, yearly...)
    
    Operations:
    - load_date_dim_from_csv(): Load từ CSV file
    - load_date_dim_with_validation(): Load với validation chi tiết
    """
    
    def __init__(self, db_conn: DatabaseConnection):
        """Khởi tạo với database connection"""
        self.db_conn = db_conn
    
    def load_date_dim_from_csv(self, csv_path: str) -> bool:
        """
        Load DateDim from CSV file (18 columns from date_dim.csv)
        
        CSV Columns:
        1. date_sk - Date surrogate key
        2. full_date - Full date (YYYY-MM-DD)
        3. day_since_2005 - Days since 2005-01-01
        4. month_since_2005 - Months since 2005-01-01
        5. day_of_week - Day name (Monday, Tuesday, etc.)
        6. calendar_month - Month name (January, February, etc.)
        7. calendar_year - Year (YYYY)
        8. calendar_year_month - Year-Month (YYYY-Mon)
        9. day_of_month - Day of month (1-31)
        10. day_of_year - Day of year (1-366)
        11. week_of_year_sunday - Week number (Sunday based)
        12. year_week_sunday - Year-Week (YYYY-Www) Sunday based
        13. week_sunday_start - Start date of week (Sunday)
        14. week_of_year_monday - Week number (Monday based)
        15. year_week_monday - Year-Week (YYYY-Www) Monday based
        16. week_monday_start - Start date of week (Monday)
        17. holiday - Holiday designation
        18. day_type - Weekend/Weekday
        
        Args:
            csv_path: Path to date_dim.csv file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            from pathlib import Path
            
            # Verify CSV file exists
            csv_file = Path(csv_path)
            if not csv_file.exists():
                logger.error(f"CSV file not found: {csv_path}")
                return False
            
            logger.info(f"Starting DateDim load from: {csv_path}")
            
            with self.db_conn.get_cursor() as cursor:
                # Clear existing data (optional, comment out if you want to preserve)
                clear_sql = f"TRUNCATE TABLE {config.Tables.DATE_DIM}"
                cursor.execute(clear_sql)
                logger.info(f"Cleared existing data from {config.Tables.DATE_DIM}")
                
                # Build column mapping for all 18 columns
                columns = (
                    "date_sk, full_date, day_since_2005, month_since_2005, "
                    "day_of_week, calendar_month, calendar_year, calendar_year_month, "
                    "day_of_month, day_of_year, week_of_year_sunday, year_week_sunday, "
                    "week_sunday_start, week_of_year_monday, year_week_monday, "
                    "week_monday_start, holiday, day_type"
                )
                
                # MySQL LOAD DATA INFILE command
                load_sql = f"""
                LOAD DATA LOCAL INFILE '{csv_path}'
                INTO TABLE {config.Tables.DATE_DIM}
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY '\\n'
                ({columns})
                """
                
                cursor.execute(load_sql)
            
            self.db_conn.connection.commit()
            
            # Verify load success
            with self.db_conn.get_cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as cnt FROM {config.Tables.DATE_DIM}")
                result = cursor.fetchone()
                row_count = result[0] if result else 0
                logger.info(f"Successfully loaded {row_count} rows into DateDim")
                
                if row_count == 0:
                    logger.warning("DateDim table is empty after load")
                    return False
            
            return True
            
        except Error as e:
            logger.error(f"Database error loading DateDim: {e}")
            if self.db_conn.connection:
                self.db_conn.connection.rollback()
            return False
        except Exception as e:
            logger.error(f"Unexpected error loading DateDim: {e}")
            if self.db_conn.connection:
                self.db_conn.connection.rollback()
            return False
    
    def load_date_dim_with_validation(self, csv_path: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Load DateDim từ CSV với validation chi tiết và error handling
        
        Quá trình load:
        1. Kiểm tra file CSV tồn tại
        2. Đọc và validate từng dòng:
           - Kiểm tra đủ 18 cột
           - Validate date_sk là số
           - Validate full_date format (YYYY-MM-DD)
        3. Clear bảng DateDim (TRUNCATE)
        4. Batch insert tất cả records hợp lệ
        5. Trả về statistics chi tiết
        
        CSV Structure (18 columns):
        1. date_sk - Surrogate key (PK)
        2. full_date - YYYY-MM-DD
        3. day_since_2005
        4. month_since_2005
        5. day_of_week - Monday, Tuesday...
        6. calendar_month - January, February...
        7. calendar_year - YYYY
        8. calendar_year_month - YYYY-Mon
        9. day_of_month - 1-31
        10. day_of_year - 1-366
        11. week_of_year_sunday
        12. year_week_sunday
        13. week_sunday_start
        14. week_of_year_monday
        15. year_week_monday
        16. week_monday_start
        17. holiday
        18. day_type - Weekend/Weekday
        
        Args:
            csv_path: Đường dẫn đến file date_dim.csv
            
        Returns:
            Tuple (success: bool, stats: Dict)
            stats chứa:
            - total_records: Tổng số dòng trong CSV
            - loaded_records: Số records load thành công
            - skipped_records: Số records bị skip (invalid)
            - errors: List các error messages
            - duration_seconds: Thời gian xử lý
        """
        stats = {
            "total_records": 0,
            "loaded_records": 0,
            "skipped_records": 0,
            "errors": [],
            "start_time": datetime.now(),
            "end_time": None,
            "duration_seconds": 0
        }
        
        try:
            from pathlib import Path
            import csv
            
            # Verify CSV file exists
            csv_file = Path(csv_path)
            if not csv_file.exists():
                error_msg = f"CSV file not found: {csv_path}"
                logger.error(error_msg)
                stats["errors"].append(error_msg)
                return False, stats
            
            logger.info(f"Starting DateDim validation load from: {csv_path}")
            
            # Read and validate CSV data
            valid_records = []
            with open(csv_path, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                
                for line_num, row in enumerate(csv_reader, 1):
                    stats["total_records"] = line_num
                    
                    # Validate row has at least 18 columns (may have more, we only use first 18)
                    if len(row) < 18:
                        error_msg = f"Line {line_num}: Expected at least 18 columns, got {len(row)}"
                        logger.warning(error_msg)
                        stats["errors"].append(error_msg)
                        stats["skipped_records"] += 1
                        continue
                    
                    # Use only first 18 columns
                    row = row[:18]
                    
                    # Validate date_sk is numeric
                    try:
                        date_sk = int(row[0])
                    except ValueError:
                        error_msg = f"Line {line_num}: Invalid date_sk '{row[0]}' (must be numeric)"
                        logger.warning(error_msg)
                        stats["errors"].append(error_msg)
                        stats["skipped_records"] += 1
                        continue
                    
                    # Validate full_date format
                    if len(row[1]) != 10 or row[1].count('-') != 2:
                        error_msg = f"Line {line_num}: Invalid date format '{row[1]}' (expected YYYY-MM-DD)"
                        logger.warning(error_msg)
                        stats["errors"].append(error_msg)
                        stats["skipped_records"] += 1
                        continue
                    
                    valid_records.append(row)
            
            if not valid_records:
                error_msg = "No valid records found in CSV file"
                logger.error(error_msg)
                stats["errors"].append(error_msg)
                return False, stats
            
            # Load valid records into database
            with self.db_conn.get_cursor() as cursor:
                # Disable foreign key checks temporarily
                cursor.execute("SET FOREIGN_KEY_CHECKS=0")
                
                # Clear existing data
                cursor.execute(f"TRUNCATE TABLE {config.Tables.DATE_DIM}")
                logger.info(f"Cleared existing data from {config.Tables.DATE_DIM}")
                
                # Prepare insert SQL
                insert_sql = f"""
                INSERT INTO {config.Tables.DATE_DIM} (
                    date_sk, full_date, day_since_2005, month_since_2005,
                    day_of_week, calendar_month, calendar_year, calendar_year_month,
                    day_of_month, day_of_year, week_of_year_sunday, year_week_sunday,
                    week_sunday_start, week_of_year_monday, year_week_monday,
                    week_monday_start, holiday, day_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                # Batch insert
                cursor.executemany(insert_sql, valid_records)
                
                # Re-enable foreign key checks
                cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            
            self.db_conn.connection.commit()
            stats["loaded_records"] = len(valid_records)
            
            logger.info(f"Successfully loaded {stats['loaded_records']} records into DateDim")
            
            stats["end_time"] = datetime.now()
            stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
            
            return True, stats
            
        except Error as e:
            error_msg = f"Database error loading DateDim: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
            if self.db_conn.connection:
                self.db_conn.connection.rollback()
            stats["end_time"] = datetime.now()
            stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
            return False, stats
        except Exception as e:
            error_msg = f"Unexpected error loading DateDim: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
            if self.db_conn.connection:
                self.db_conn.connection.rollback()
            stats["end_time"] = datetime.now()
            stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
            return False, stats
