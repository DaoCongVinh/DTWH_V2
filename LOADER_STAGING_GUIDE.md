# 📘 LOADER STAGING - HƯỚNG DẪN TOÀN DIỆN

## 📑 Mục Lục
1. [Tổng Quan](#tổng-quan)
2. [Kiến Trúc Hệ Thống](#kiến-trúc-hệ-thống)
3. [Quy Trình Chi Tiết](#quy-trình-chi-tiết)
4. [Cài Đặt & Setup](#cài-đặt--setup)
5. [Sử Dụng](#sử-dụng)
6. [Monitoring](#monitoring)
7. [Troubleshooting](#troubleshooting)

---

## 🎯 Tổng Quan

**Loader Staging** là hệ thống ETL chuyên dụng để xử lý dữ liệu TikTok từ APIFY Crawler.

### Mục Đích Chính
- ✅ Validate JSON từ Crawler
- ✅ Lưu audit trail (RawJson)
- ✅ Chuẩn hóa dữ liệu thành 3 bảng staging
- ✅ Tối ưu ETL bằng batch processing
- ✅ Giữ lịch sử dữ liệu (SCD Type 2)
- ✅ Ghi log đầy đủ cho tracking

### Đầu Vào & Đầu Ra

```
INPUT                          PROCESS                    OUTPUT
/data/storage/*.json ------→ [Loader Staging] ----→ dbStaging
(từ APIFY Crawler)          [Validate]              (6 bảng)
                            [Transform]
                            [Upsert]
                            [Log]
```

---

## 🏗️ Kiến Trúc Hệ Thống

### Cấu Trúc Thư Mục

```
d:\DTWH_V2\
├── services\
│   ├── crawler\                    # APIFY Crawler service
│   ├── loaderStaging\              # ← LOADER STAGING
│   │   ├── config.py               # Configuration
│   │   ├── db.py                   # Database operations
│   │   ├── loader.py               # Main orchestrator
│   │   ├── logging_setup.py        # Logging config
│   │   ├── schema_dbStaging.sql    # Database schema
│   │   ├── tiktok_schema.json      # JSON schema
│   │   ├── date_dim.csv            # Date dimension data
│   │   ├── requirements.txt        # Dependencies
│   │   ├── Dockerfile              # Docker image
│   │   ├── setup.sh                # Setup script
│   │   └── README.md               # Documentation
│   └── transformer\                # Transformer service
├── storage\                        # Data storage
│   ├── *.json                      # Raw JSON files
│   ├── processed\                  # Processed files
│   └── failed\                     # Failed files
├── docker-compose.yml              # Docker compose config
└── .env                            # Environment variables
```

### Database Schema (dbStaging)

```
┌─────────────────────────────────────────────────────┐
│                    dbStaging                        │
├─────────────────────────────────────────────────────┤
│                                                     │
│ ┌─────────────┐      ┌──────────────┐              │
│ │ RawJson     │      │ DateDim      │              │
│ │ (Audit)     │      │ (Dimension)  │              │
│ └─────────────┘      └──────────────┘              │
│        │                    │                       │
│        └────────┬───────────┘                       │
│                 │                                  │
│        ┌────────▼────────┐                         │
│        │   LoadLog       │                         │
│        │ (Audit Trail)   │                         │
│        └─────────────────┘                         │
│                 │                                  │
│   ┌─────────────┼─────────────┐                    │
│   │             │             │                    │
│ ┌─▼──────┐  ┌──▼─────┐   ┌───▼───────┐            │
│ │Authors │  │Videos  │   │Interactions│           │
│ │(SCD2)  │  │(SCD2)  │   │(SCD2)     │           │
│ └────────┘  └────────┘   └───────────┘            │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Module Architecture

```python
┌────────────────────────────────────────┐
│         loader.py (Main)               │
│  - JSONValidator                       │
│  - DataTransformer                     │
│  - TikTokLoader                        │
│  - LoaderScheduler                     │
└──────────────┬─────────────────────────┘
               │
       ┌───────┴────────┬────────────┐
       │                │            │
   ┌───▼───┐        ┌───▼──┐    ┌───▼───────┐
   │db.py  │        │conf. │    │logging.py │
   │       │        │py    │    │           │
   └───────┘        └──────┘    └───────────┘
   │
   ├─ DatabaseConnection
   ├─ BatchFetcher
   ├─ RawJsonManager
   ├─ UpsertManager
   ├─ LoadLogManager
   └─ DateDimManager
```

---

## 🔄 Quy Trình Chi Tiết

### Step 1: Input & Validation

```
File: device-unknown_run_23112025T042307Z.json
Size: ~500KB
Items: 100-1000

  ┌──────────────────────────────┐
  │ 1. Read JSON File            │
  │    - Đọc content             │
  │    - Parse JSON              │
  └──────────┬───────────────────┘
             │
  ┌──────────▼───────────────────┐
  │ 2. Validate JSON Schema      │
  │    - Check required fields   │
  │    - Validate types          │
  │    - Validate ranges         │
  └──────────┬───────────────────┘
             │
         ┌───┴────┐
         │        │
    ✅ VALID   ❌ INVALID
         │        │
         │        └──→ RawJson(FAILED)
         │            └──→ /failed/
         │
  ┌──────▼────────────────────────┐
  │ 3. Save Raw JSON              │
  │    - Store full content       │
  │    - Store filename           │
  │    - Store timestamp          │
  └──────────────────────────────┘
```

### Step 2: Data Preparation

```
  ┌────────────────────────────────┐
  │ 1. Load DateDim                │
  │    - Read date_dim.csv         │
  │    - Load into DateDim table   │
  │    - Get today's date_sk       │
  │    Result: date_sk = 1         │
  └────────────┬───────────────────┘
               │
  ┌────────────▼───────────────────┐
  │ 2. Batch Fetch Cache Data      │
  │    - Fetch all authors (Q1)    │
  │    - Fetch all videos (Q2)     │
  │    - Fetch all interactions(Q3)│
  │    Result: 3 sets in memory    │
  └────────────────────────────────┘
```

### Step 3: Transform

```
Raw JSON Item:
{
  "id": "video_123",
  "text": "#fyp #viral",
  "createTime": 1700000000,
  "authorMeta": {
    "id": "author_456",
    "name": "user123",
    "avatar": "..."
  },
  "videoMeta": { "duration": 15 },
  "diggCount": 5000,
  ...
}

  ┌─────────────────────────────────────┐
  │ Extract 3 Structures                │
  ├─────────────────────────────────────┤
  │                                     │
  │ Author:                             │
  │ ├─ author_id: "author_456"         │
  │ ├─ author_name: "user123"          │
  │ ├─ avatar: "..."                   │
  │ └─ extract_date_sk: 1              │
  │                                     │
  │ Video:                              │
  │ ├─ video_id: "video_123"           │
  │ ├─ author_id: "author_456"         │
  │ ├─ text_content: "#fyp #viral"    │
  │ ├─ duration: 15                    │
  │ ├─ create_time: "2025-11-15 ..."  │
  │ └─ create_date_sk: 1               │
  │                                     │
  │ Interaction:                        │
  │ ├─ video_id: "video_123"           │
  │ ├─ digg_count: 5000                │
  │ ├─ play_count: 50000               │
  │ ├─ share_count: 500                │
  │ └─ interaction_date_sk: 1          │
  │                                     │
  └─────────────────────────────────────┘
```

### Step 4: Upsert (SCD Type 2)

```
For each Author:
  IF author_id NOT IN existing_authors
    → INSERT new author
  ELSE IF author_id EXISTS
    IF author_name = old AND avatar = old
      → SKIP (không thay đổi)
    ELSE
      IF extract_date_sk (hiện tại) = today_sk
        → UPDATE (cập nhật cùng ngày)
      ELSE
        → INSERT (tạo version mới, giữ lịch sử)

Example Timeline:
───────────────────────────────────────────────
2025-11-23 (date_sk=1): author_456 (avatar_v1)
2025-11-24 (date_sk=2): author_456 (avatar_v2) ← new version
2025-11-24 (date_sk=2): author_456 (avatar_v3) ← UPDATE (same day)
───────────────────────────────────────────────

Query results:
author_id | avatar  | extract_date_sk | is_current
author_456| v1      | 1               | FALSE
author_456| v3      | 2               | TRUE
```

### Step 5: Logging & File Management

```
  ┌──────────────────────────────┐
  │ Process Complete             │
  ├──────────────────────────────┤
  │                              │
  │ Log Statistics:              │
  │ ├─ batch_id: LOAD_20251123...│
  │ ├─ Authors:                  │
  │ │  ├─ inserted: 50           │
  │ │  ├─ updated: 10            │
  │ │  └─ skipped: 40            │
  │ ├─ Videos: ...               │
  │ ├─ Interactions: ...         │
  │ └─ Status: SUCCESS           │
  │                              │
  │ File Movement:               │
  │ ├─ SUCCESS: /processed/      │
  │ └─ FAILED: /failed/          │
  │                              │
  └──────────────────────────────┘
```

---

## 💻 Cài Đặt & Setup

### Yêu Cầu
- Docker & Docker Compose
- Python 3.11+
- MySQL 8.0+
- 500MB disk space (logs, staging)

### Bước 1: Clone & Config

```bash
# 1. Clone repository
git clone <repo>
cd DTWH_V2

# 2. Copy environment variables
cp .env.example .env

# 3. Edit .env
nano .env
# Thay đổi:
# MYSQL_ROOT_PASSWORD
# MYSQL_USER, MYSQL_PASSWORD
# STORAGE_PATH (nếu cần)
```

### Bước 2: Create Database Schema

```bash
# Option 1: Sử dụng setup script
bash services/loaderStaging/setup.sh

# Option 2: Manual
docker-compose up -d db
sleep 5
docker-compose exec -T db mysql -u root -p"$MYSQL_ROOT_PASSWORD" < services/loaderStaging/schema_dbStaging.sql
```

### Bước 3: Load DateDim

```bash
# Tự động (qua Docker)
docker-compose exec -T db mysql -u root -p"$MYSQL_ROOT_PASSWORD" dbStaging < services/loaderStaging/date_dim.csv

# Hoặc manual
docker-compose exec db mysql -u root -p
mysql> LOAD DATA LOCAL INFILE '/path/to/date_dim.csv' INTO TABLE DateDim ...
```

### Bước 4: Build & Start Loader

```bash
# Build image
docker build -t dtwh-loader-staging ./services/loaderStaging

# Start services
docker-compose up -d db
docker-compose up -d loader-staging

# Verify
docker-compose ps
docker-compose logs -f loader-staging
```

---

## 🚀 Sử Dụng

### CLI Commands

```bash
# 1. Full Pipeline (Default)
python loader.py

# 2. Chỉ Load Raw JSON
python loader.py --load_raw

# 3. Chỉ Load Staging
python loader.py --load_staging

# 4. Không di chuyển file
python loader.py --no-remove

# 5. Chạy with Scheduler
python loader.py --schedule

# 6. Kết hợp
python loader.py --load_raw --no-remove --schedule
```

### Docker Commands

```bash
# Run one-time
docker run --rm \
  --network dtwh_v2_tiktok_network \
  -e MYSQL_HOST=db \
  -v /data/storage:/data/storage \
  dtwh-loader-staging python loader.py

# Run with compose
docker-compose up loader-staging
docker-compose logs -f loader-staging

# Stop
docker-compose down

# Clean (remove volumes)
docker-compose down -v
```

### Monitoring in Real-time

```bash
# Watch logs
docker-compose logs -f loader-staging --tail=100

# Query database while running
docker-compose exec db mysql -u root -p dbStaging -e \
  "SELECT * FROM LoadLog ORDER BY created_at DESC LIMIT 5;"

# Count records
docker-compose exec db mysql -u root -p dbStaging -e \
  "SELECT table_name, COUNT(*) as count FROM LoadLog GROUP BY table_name;"
```

---

## 📊 Monitoring

### Database Queries

```sql
-- 1. Xem lịch sử load
SELECT 
  batch_id, 
  table_name, 
  record_count, 
  inserted_count, 
  updated_count, 
  status,
  duration_seconds
FROM LoadLog 
ORDER BY created_at DESC 
LIMIT 10;

-- 2. Xem failed files
SELECT 
  filename, 
  load_status, 
  error_message,
  loaded_at
FROM RawJson 
WHERE load_status = 'FAILED' 
ORDER BY loaded_at DESC;

-- 3. Xem authors hôm nay
SELECT COUNT(*) as total_authors
FROM Authors 
WHERE extract_date_sk = (SELECT date_sk FROM DateDim WHERE CURDATE() = full_date);

-- 4. Xem video updates
SELECT 
  video_id,
  MAX(create_date_sk) as latest_date_sk,
  COUNT(*) as versions
FROM Videos
GROUP BY video_id
HAVING COUNT(*) > 1
ORDER BY latest_date_sk DESC;

-- 5. Statistics
SELECT 
  COUNT(DISTINCT author_id) as unique_authors,
  COUNT(DISTINCT video_id) as unique_videos,
  COUNT(*) as total_interactions
FROM VideoInteractions;
```

### Log Analysis

```bash
# Recent errors
tail -50 logs/loader.log | grep ERROR

# Count by level
grep INFO logs/loader.log | wc -l
grep ERROR logs/loader.log | wc -l
grep WARNING logs/loader.log | wc -l

# Monitor in real-time
tail -f logs/loader.log
```

---

## 🔧 Troubleshooting

### Issue 1: Database Connection Failed

```
Error: Database connection error: ...
```

**Nguyên nhân**: MySQL service chưa ready

**Giải pháp**:
```bash
# Kiểm tra database
docker-compose ps db

# Restart database
docker-compose restart db
sleep 10

# Run loader lại
docker-compose up loader-staging
```

### Issue 2: JSON Validation Failed

```
Error: JSON validation failed: ...
```

**Nguyên nhân**: File JSON không khớp schema

**Giải pháp**:
```bash
# Kiểm tra file trong /failed
ls storage/failed/

# Xem chi tiết error
docker-compose exec db mysql -u root -p dbStaging -e \
  "SELECT filename, error_message FROM RawJson WHERE load_status='FAILED';"

# Kiểm tra schema
cat services/loaderStaging/tiktok_schema.json
```

### Issue 3: Today's Date Not Found

```
Error: Failed to get today's date_sk. Aborting.
```

**Nguyên nhân**: DateDim chưa được load hoặc không có ngày hôm nay

**Giải pháp**:
```bash
# Check DateDim
docker-compose exec db mysql -u root -p dbStaging -e \
  "SELECT COUNT(*) FROM DateDim;"

# If empty, load data
docker-compose exec -T db mysql -u root -p"$MYSQL_ROOT_PASSWORD" dbStaging << EOF
LOAD DATA LOCAL INFILE '/app/date_dim.csv'
INTO TABLE DateDim
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(date_sk, full_date, year, month, day);
EOF

# Verify today's date
docker-compose exec db mysql -u root -p dbStaging -e \
  "SELECT date_sk FROM DateDim WHERE full_date = CURDATE();"
```

### Issue 4: File Not Moving

```
No files moving to /processed or /failed
```

**Nguyên nhân**: Permission issue hoặc path không tồn tại

**Giải pháp**:
```bash
# Create directories
mkdir -p storage/processed storage/failed
chmod 777 storage/processed storage/failed

# Check permissions
ls -la storage/

# Re-run loader with --no-remove để test
python loader.py --no-remove
```

### Issue 5: Out of Memory

```
Error: Memory error when processing large file
```

**Nguyên nhân**: File quá lớn hoặc batch fetch quá nhiều data

**Giải pháp**:
```bash
# Option 1: Increase container memory
# In docker-compose.yml
services:
  loader-staging:
    mem_limit: 2g  # Increase from default

# Option 2: Process fewer files at once
# Add cronjob to process one file per minute

# Option 3: Reduce MAX_ITEMS_PER_BATCH in config.py
MAX_ITEMS_PER_BATCH = 500  # Default: 1000
```

---

## 🎓 Best Practices

### 1. Scheduler Configuration

```bash
# Mỗi 1 phút
LOADER_SCHEDULE_CRON=0 */1 * * *

# Mỗi 5 phút
LOADER_SCHEDULE_CRON=0 */5 * * *

# Mỗi giờ (vào phút thứ 0)
LOADER_SCHEDULE_CRON=0 0 * * *

# Mỗi ngày lúc 1:00 AM
LOADER_SCHEDULE_CRON=0 1 * * *
```

### 2. Error Recovery

```bash
# 1. Kiểm tra logs
tail -100 logs/loader.log

# 2. Xem failed files
ls storage/failed/

# 3. Move file back để reprocess
mv storage/failed/file.json storage/

# 4. Fix issue và re-run
python loader.py
```

### 3. Data Backup

```bash
# Backup database
docker-compose exec db mysqldump -u root -p dbStaging > backup_$(date +%Y%m%d).sql

# Backup files
tar -czf storage_backup_$(date +%Y%m%d).tar.gz storage/

# Restore
docker-compose exec -T db mysql -u root -p < backup_20251123.sql
tar -xzf storage_backup_20251123.tar.gz
```

### 4. Monitoring Setup

```bash
# Log monitoring script
#!/bin/bash
while true; do
  clear
  echo "=== Loader Status ==="
  docker-compose ps loader-staging
  echo ""
  echo "=== Recent Logs ==="
  tail -10 logs/loader.log
  echo ""
  echo "=== Database Stats ==="
  docker-compose exec -T db mysql -u root -p"$MYSQL_ROOT_PASSWORD" dbStaging -e \
    "SELECT COUNT(*) FROM Authors; SELECT COUNT(*) FROM Videos; SELECT COUNT(*) FROM VideoInteractions;"
  sleep 10
done
```

---

## 📚 References

| File | Purpose |
|------|---------|
| `config.py` | Configuration & constants |
| `db.py` | Database operations |
| `loader.py` | Main orchestrator |
| `tiktok_schema.json` | JSON validation schema |
| `schema_dbStaging.sql` | Database schema |
| `docker-compose.yml` | Docker configuration |
| `.env` | Environment variables |

---

## ✅ Checklist: First Run

- [ ] .env configured
- [ ] Docker daemon running
- [ ] Database schema created
- [ ] DateDim loaded
- [ ] Directories created (processed, failed)
- [ ] Sample JSON file in `/data/storage`
- [ ] Loader started
- [ ] Logs visible
- [ ] Data in database
- [ ] File moved to /processed

---

Chúc bạn cài đặt thành công! 🚀

