# TikTok Loader Staging Service

Loader Staging là dịch vụ ETL (Extract, Transform, Load) toàn diện để xử lý dữ liệu TikTok từ APIFY Crawler.

## 📋 Quy Trình Hoạt Động

### 1. **Input**: Đọc file JSON từ Crawler
- Đường dẫn: `/data/storage/*.json`
- Dữ liệu từ APIFY Crawler chứa: Author, Video, Stats (Interactions)

### 2. **Validate**: Kiểm tra JSON Schema
- Sử dụng `tiktok_schema.json` để validate cấu trúc
- Nếu invalid → Lưu vào RawJson với trạng thái FAILED → Move vào `/data/storage/failed`
- Nếu valid → Tiếp tục

### 3. **Save Raw**: Lưu dữ liệu thô vào RawJson
- Insert toàn bộ JSON content vào bảng `RawJson`
- Tạo audit trail đầy đủ (file name, timestamp, status)

### 4. **Load DateDim**: Load bảng ngày tháng
- Đọc `date_dim.csv` → Load vào bảng `DateDim`
- Lấy `date_sk` của ngày hôm nay (dùng cho SCD)

### 5. **Transform**: Chuẩn hóa JSON → 3 bảng
- **Authors**: authorID, Name, avatar, extract_date_sk
- **Videos**: videoID, authorID, TextContent, Duration, CreateTime, WebVideoUrl, create_date_sk
- **VideoInteractions**: videoID, DiggCount, PlayCount, ShareCount, CommentCount, CollectCount, interaction_date_sk

### 6. **Batch Fetch**: Tối ưu ETL
- 1 query: Fetch tất cả author IDs
- 1 query: Fetch tất cả video IDs
- 1 query: Fetch tất cả video IDs trong interactions
- → Giảm từ N queries xuống 3 queries

### 7. **Upsert (SCD Type 2)**
```
IF bản ghi chưa tồn tại
    → INSERT

ELSE IF bản ghi tồn tại
    IF dữ liệu không thay đổi
        → SKIP
    ELSE IF dữ liệu thay đổi
        → UPDATE (cùng ngày) hoặc INSERT (khác ngày, giữ lịch sử)
```

### 8. **Log**: Ghi vào bảng LoadLog
- batch_id, table_name, record_count, status, start_time, end_time, duration

### 9. **Move**: Di chuyển file
- SUCCESS → `/data/storage/processed`
- FAILED → `/data/storage/failed`

---

## 🗄️ Schema Database

### Bảng RawJson
```sql
raw_json_id (PK)
content (JSON đầy đủ)
filename
load_status (SUCCESS/FAILED)
loaded_at
error_message
```

### Bảng DateDim
```sql
date_sk (PK, Surrogate Key)
full_date (e.g., 2025-11-23)
year
month
day
```

### Bảng Authors (SCD Type 2)
```sql
author_id (PK)
author_name
avatar
extract_date_sk (PK, FK)
is_current
created_at
updated_at
```

### Bảng Videos (SCD Type 2)
```sql
video_id (PK)
author_id (FK)
text_content
duration
create_time
web_video_url
create_date_sk (PK, FK)
is_current
```

### Bảng VideoInteractions (SCD Type 2)
```sql
interaction_id (PK, AUTO_INCREMENT)
video_id (FK, UNIQUE)
digg_count
play_count
share_count
comment_count
collect_count
interaction_date_sk (FK, UNIQUE)
is_current
```

### Bảng LoadLog (Audit Trail)
```sql
log_id (PK)
batch_id
table_name
record_count
inserted_count
updated_count
skipped_count
status (SUCCESS/FAILED/PARTIAL)
start_time
end_time
duration_seconds
source_filename
error_message
```

---

## 🚀 Cách Sử Dụng

### Setup Ban Đầu

1. **Tạo Database**
```bash
cd services/loaderStaging
docker-compose exec db mysql -u root -p < schema_dbStaging.sql
```

2. **Cài Dependencies**
```bash
pip install -r requirements.txt
```

3. **Kiểm tra Config**
```bash
# File: config.py
MYSQL_HOST = "db"          # Database host
MYSQL_USER = "loader_user" # Database user
MYSQL_PASSWORD = "password"
STORAGE_PATH = "/data/storage"
DATE_DIM_PATH = "./date_dim.csv"
```

### Chạy Loader - Các Mode

#### Mode 1: Full Pipeline (Đầy đủ)
```bash
python loader.py
```
- Validate JSON
- Load raw JSON
- Load staging tables
- Move file

#### Mode 2: Chỉ Load Raw JSON
```bash
python loader.py --load_raw
```
- Chỉ validate + lưu raw JSON
- Skip staging tables

#### Mode 3: Chỉ Load Staging
```bash
python loader.py --load_staging
```
- Skip raw JSON
- Chỉ xử lý staging tables

#### Mode 4: Không Di Chuyển File
```bash
python loader.py --no-remove
```
- Xử lý đầy đủ
- Nhưng giữ file trong `/data/storage`

#### Mode 5: Chạy với Scheduler
```bash
python loader.py --schedule
```
- Chạy tự động theo cron
- Default: `0 */1 * * *` (mỗi 1 phút)

#### Kết Hợp Mode
```bash
# Load raw + staging, giữ file
python loader.py --no-remove

# Chỉ test raw JSON
python loader.py --load_raw --no-remove
```

---

## 🐳 Docker

### Build Image
```bash
docker build -t dtwh-loader-staging ./services/loaderStaging
```

### Run Container
```bash
docker run --rm \
  -e MYSQL_HOST=db \
  -e MYSQL_USER=loader_user \
  -e MYSQL_PASSWORD=password \
  -e STORAGE_PATH=/data/storage \
  -v $(pwd)/storage:/data/storage \
  dtwh-loader-staging
```

### Docker Compose
```bash
docker-compose up loader-staging
```

---

## 📊 Monitoring & Logs

### Log File
```
logs/loader.log
```

### Query Logs
```sql
-- Xem lịch sử load
SELECT * FROM LoadLog ORDER BY created_at DESC LIMIT 10;

-- Xem các file failed
SELECT * FROM RawJson WHERE load_status = 'FAILED';

-- Xem dữ liệu authors hôm nay
SELECT * FROM Authors WHERE extract_date_sk = (SELECT date_sk FROM DateDim WHERE full_date = CURDATE());

-- Xem thống kê videos
SELECT COUNT(*) as total, COUNT(DISTINCT author_id) as unique_authors FROM Videos;
```

---

## ⚙️ Configuration

### Environment Variables
```bash
# Database
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=rootpass
MYSQL_DATABASE=dbStaging

# Storage
STORAGE_PATH=/data/storage
DATE_DIM_PATH=./date_dim.csv

# Scheduler
SCHEDULE_ENABLED=True
SCHEDULE_CRON=0 */1 * * *

# Application
APP_ENV=production
LOG_LEVEL=INFO
DEBUG_MODE=False
```

### config.py Constants
```python
# Loader Modes
LoaderMode.FULL           # Full pipeline
LoaderMode.RAW_ONLY       # Only raw JSON
LoaderMode.STAGING_ONLY   # Only staging
LoaderMode.NO_REMOVE      # Keep files
LoaderMode.SCHEDULE       # With scheduler

# Validation Limits
Validation.MAX_ITEMS_PER_FILE = 10000
Validation.MAX_STAT_COUNT = 9999999999
Validation.MAX_DURATION = 600  # seconds
```

---

## 🔍 Troubleshooting

### Error: "Today's date not found in DateDim"
**Nguyên nhân**: DateDim chưa được load
**Giải pháp**: 
```sql
LOAD DATA LOCAL INFILE '/app/date_dim.csv'
INTO TABLE DateDim
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
```

### Error: "JSON validation failed"
**Nguyên nhân**: File JSON không khớp schema
**Giải pháp**:
- Kiểm tra file trong `/data/storage/failed`
- Xem chi tiết lỗi trong LoadLog
- Kiểm tra tiktok_schema.json

### Error: "Database connection error"
**Nguyên nhân**: MySQL chưa ready
**Giải pháp**:
```bash
# Chờ database start
docker-compose up -d db
sleep 10
docker-compose up loader-staging
```

### Error: "Duplicate entry"
**Nguyên nhân**: Primary key conflict
**Giải pháp**: 
- Kiểm tra logic upsert
- Xem LoadLog để debug
- Reset tables nếu cần

---

## 📈 Performance Tips

1. **Batch Fetch**: Loader tự động fetch toàn bộ data vào memory
   - Giảm từ N queries xuống 3 queries
   - Tối ưu cho dataset nhỏ đến trung bình (<100K records)

2. **Scheduler**: Chạy mỗi 1 phút
   - Tránh load quá nhiều files cùng lúc
   - Có thể adjust cron expression

3. **SCD Type 2**: Giữ lịch sử đầy đủ
   - Hỗ trợ audit trail
   - Tìm nguyên nhân thay đổi dữ liệu

4. **Raw JSON**: Audit trail mọi file
   - Validate trước insert
   - Log error chi tiết

---

## 🧪 Testing

### Test File
```json
[{
  "id": "7562365363220925703",
  "text": "#test #video",
  "createTime": 1700000000,
  "authorMeta": {
    "id": "test_author_001",
    "name": "test_user",
    "avatar": "https://..."
  },
  "videoMeta": {
    "duration": 10,
    "width": 576,
    "height": 1024
  },
  "webVideoUrl": "https://tiktok.com/@test/video/123",
  "diggCount": 1000,
  "playCount": 50000,
  "shareCount": 100,
  "commentCount": 50,
  "collectCount": 200
}]
```

### Manual Test
```bash
# Copy test file
cp test_video.json /data/storage/

# Run loader
python loader.py

# Check results
docker-compose exec db mysql -u root -ppassword dbStaging -e "SELECT COUNT(*) FROM Authors;"
```

---

## 📝 File Structure

```
services/loaderStaging/
├── config.py                 # Configuration & constants
├── db.py                     # Database operations
├── loader.py                 # Main orchestrator
├── logging_setup.py          # Logging configuration
├── schema_dbStaging.sql      # Database schema
├── tiktok_schema.json        # JSON validation schema
├── date_dim.csv              # Date dimension data
├── requirements.txt          # Python dependencies
├── Dockerfile                # Docker image definition
└── README.md                 # This file
```

---

## 📚 References

- JSON Schema: `tiktok_schema.json`
- Database Schema: `schema_dbStaging.sql`
- Config: `config.py`
- DB Helper: `db.py`
- Main Logic: `loader.py`

---

## 🤝 Support

- Logs: `logs/loader.log`
- Database logs: MySQL error log
- Issues: Kiểm tra LoadLog table

