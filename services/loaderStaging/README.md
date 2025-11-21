# Loader Staging Service

Service này chịu trách nhiệm tải dữ liệu JSON raw từ storage vào database staging MySQL, bao gồm:
- Tạo schema và thủ tục SQL
- Load DateDim từ CSV
- Xử lý JSON files và insert vào bảng Authors, Videos, VideoInteractions
- Fallback Python nếu thủ tục SQL gặp lỗi

## Setup

### 1. Database Requirements
- MySQL 5.7+ hoặc 8.0+
- Bật `local_infile` nếu muốn dùng LOAD DATA LOCAL INFILE:
  ```sql
  SET GLOBAL local_infile=1;
  ```

### 2. Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Environment Configuration
Copy `.env.example` thành `.env` và cập nhật:
```bash
cp .env.example .env
```

Chỉnh sửa `.env`:
```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=your_user
MYSQL_PASSWORD=your_password
MYSQL_DATABASE_STAGING=dbStaging
STORAGE_PATH=../../storage
```

### 4. Test Connection
```bash
python test_loader.py
```

### 5. Run Full Pipeline
```bash
python loader.py
```

## Files

- `loader.py` - Main pipeline script
- `loader.sql` - Schema và stored procedures  
- `date_dim.csv` - Date dimension data
- `test_loader.py` - Test functions
- `requirements.txt` - Python dependencies

## Troubleshooting

### DateDim không load được

**Nguyên nhân phổ biến:**
1. Cột `quarter` trong CSV có dạng "2005-Q01" nhưng bảng cần INT
2. Thủ tục SQL `load_date_dim_from_csv` không tồn tại
3. `local_infile` chưa bật trên MySQL server
4. Quyền file path không đúng

**Giải pháp:**
- Service tự động fallback sang Python nếu thủ tục SQL lỗi
- Fallback sẽ convert "2005-Q01" → 1, "2005-Q02" → 2, etc.

### Process Raw Record lỗi

**Nguyên nhân:**
- JSON structure không match với expected format
- Thiếu video ID hoặc author ID
- Timestamp conversion lỗi

**Debug:**
```bash
python test_loader.py
```

### Connection lỗi

**Kiểm tra:**
1. MySQL service đang chạy
2. Credentials trong `.env` đúng
3. Database `dbStaging` đã tạo
4. User có quyền CREATE, INSERT, UPDATE

## Schema Overview

### Core Tables
- `Authors` - Author information với date dimension
- `Videos` - Video metadata với date dimension  
- `VideoInteractions` - Interaction stats với date dimension
- `DateDim` - Date dimension table
- `RawJson` - Raw JSON storage cho audit trail
- `LoadLog` - ETL audit logging

### Stored Procedures
- `insert_load_log()` - Ghi log ETL operations
- `process_raw_record()` - Xử lý 1 JSON record thành normalized tables
- `load_date_dim_from_csv()` - Load DateDim từ CSV với quarter conversion

## Data Flow

1. **Schema Creation**: Tạo database, tables, procedures từ `loader.sql`
2. **Raw Processing**: Đọc JSON files từ `STORAGE_PATH`, gọi `process_raw_record()` cho mỗi record
3. **DateDim Load**: Load date dimension từ CSV, fallback Python nếu thủ tục SQL lỗi  
4. **Logging**: Ghi kết quả vào `LoadLog` table

## Architecture Notes

- **Fault Tolerance**: Fallback Python khi SQL procedures lỗi
- **Idempotency**: UPSERT operations cho repeated runs
- **Audit Trail**: Mọi operations đều ghi log
- **Data Validation**: Skip records thiếu ID, convert types safely