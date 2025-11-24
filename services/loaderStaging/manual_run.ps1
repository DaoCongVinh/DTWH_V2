# Complete Manual Run Script
# File: manual_run.ps1
# Purpose: Run LoaderStaging manually without Docker

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  TikTok LoaderStaging - Manual Run Script" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$WORKSPACE_ROOT = "D:\DTWH_V21\DTWH_V2"
$LOADER_DIR = "$WORKSPACE_ROOT\services\loaderStaging"
$STORAGE_DIR = "$WORKSPACE_ROOT\storage"
$MYSQL_HOST = "localhost"
$MYSQL_USER = "user"
$MYSQL_PASSWORD = "dwhtiktok"
$MYSQL_DATABASE = "dbStaging"

# Step 1: Set environment variables
Write-Host "[Step 1/8] Setting environment variables..." -ForegroundColor Yellow
$env:MYSQL_HOST = $MYSQL_HOST
$env:MYSQL_PORT = "3306"
$env:MYSQL_USER = $MYSQL_USER
$env:MYSQL_PASSWORD = $MYSQL_PASSWORD
$env:MYSQL_DATABASE = $MYSQL_DATABASE
$env:STORAGE_PATH = $STORAGE_DIR
$env:DATE_DIM_PATH = "$LOADER_DIR\date_dim.csv"
$env:LOG_LEVEL = "INFO"
$env:LOG_FILE = "$LOADER_DIR\loader.log"
Write-Host "  ✓ Environment configured" -ForegroundColor Green
Write-Host ""

# Step 2: Check Python
Write-Host "[Step 2/8] Checking Python installation..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "  ✓ Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "  ✗ ERROR: Python not found!" -ForegroundColor Red
    Write-Host "  Please install Python 3.11 or higher" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Step 3: Check MySQL connection
Write-Host "[Step 3/8] Testing MySQL connection..." -ForegroundColor Yellow
$mysqlTest = mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1 as test;" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ✗ ERROR: Cannot connect to MySQL!" -ForegroundColor Red
    Write-Host "  Connection details:" -ForegroundColor Red
    Write-Host "    Host: $MYSQL_HOST" -ForegroundColor Red
    Write-Host "    User: $MYSQL_USER" -ForegroundColor Red
    Write-Host "    Database: $MYSQL_DATABASE" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Possible solutions:" -ForegroundColor Yellow
    Write-Host "    1. Check if MySQL service is running: Get-Service MySQL*" -ForegroundColor Yellow
    Write-Host "    2. Start MySQL: net start MySQL80" -ForegroundColor Yellow
    Write-Host "    3. Or start Docker: docker-compose up -d db" -ForegroundColor Yellow
    exit 1
}
Write-Host "  ✓ MySQL connected successfully" -ForegroundColor Green
Write-Host ""

# Step 4: Check database schema
Write-Host "[Step 4/8] Checking database schema..." -ForegroundColor Yellow
$tableCheck = mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE -e "SHOW TABLES;" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ✗ ERROR: Database '$MYSQL_DATABASE' not found or empty!" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Creating database schema..." -ForegroundColor Yellow
    Get-Content "$WORKSPACE_ROOT\init_db\schema.sql" | mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✓ Schema created successfully" -ForegroundColor Green
    } else {
        Write-Host "  ✗ ERROR: Failed to create schema!" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "  ✓ Database schema exists" -ForegroundColor Green
}
Write-Host ""

# Step 5: Create storage directories
Write-Host "[Step 5/8] Creating storage directories..." -ForegroundColor Yellow
@($STORAGE_DIR, "$STORAGE_DIR\processed", "$STORAGE_DIR\failed") | ForEach-Object {
    if (!(Test-Path $_)) {
        New-Item -ItemType Directory -Force -Path $_ | Out-Null
        Write-Host "  + Created: $_" -ForegroundColor Cyan
    }
}
Write-Host "  ✓ Storage directories ready" -ForegroundColor Green
Write-Host ""

# Step 6: Load DateDim
Write-Host "[Step 6/8] Loading DateDim data..." -ForegroundColor Yellow
Set-Location $LOADER_DIR

# Check if DateDim already has data
$dateDimCount = mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE -e "SELECT COUNT(*) FROM DateDim;" 2>&1 | Select-Object -Last 1
if ($dateDimCount -gt 0) {
    Write-Host "  ℹ DateDim already loaded ($dateDimCount records)" -ForegroundColor Cyan
} else {
    Write-Host "  Loading DateDim from CSV..." -ForegroundColor Cyan
    python load_date_dim_cli.py --simple 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✓ DateDim loaded successfully" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ WARNING: DateDim load had issues (check logs)" -ForegroundColor Yellow
    }
}
Write-Host ""

# Step 7: Check JSON files
Write-Host "[Step 7/8] Scanning for JSON files..." -ForegroundColor Yellow
$jsonFiles = Get-ChildItem $STORAGE_DIR -Filter *.json -ErrorAction SilentlyContinue
if ($jsonFiles.Count -eq 0) {
    Write-Host "  ⚠ WARNING: No JSON files found in $STORAGE_DIR" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Creating a test JSON file..." -ForegroundColor Cyan
    
    $testJson = @'
[
  {
    "id": "7123456789012345678",
    "text": "Manual run test video - PowerShell",
    "createTime": 1732435200,
    "webVideoUrl": "https://www.tiktok.com/@manual_test/video/123",
    "authorMeta": {
      "id": "7234567890",
      "name": "manual_test_user",
      "avatar": "https://example.com/avatar.jpg"
    },
    "videoMeta": {
      "duration": 15
    },
    "diggCount": 150,
    "playCount": 2500,
    "shareCount": 25,
    "commentCount": 10,
    "collectCount": 30
  }
]
'@
    
    $testJson | Out-File -FilePath "$STORAGE_DIR\manual_test.json" -Encoding utf8
    Write-Host "  ✓ Created test file: manual_test.json" -ForegroundColor Green
    $jsonFiles = Get-ChildItem $STORAGE_DIR -Filter *.json
}

Write-Host "  ✓ Found $($jsonFiles.Count) JSON file(s) to process:" -ForegroundColor Green
$jsonFiles | ForEach-Object {
    Write-Host "    - $($_.Name)" -ForegroundColor Cyan
}
Write-Host ""

# Step 8: Run loader
Write-Host "[Step 8/8] Running TikTok Loader..." -ForegroundColor Yellow
Write-Host "  Starting loader process..." -ForegroundColor Cyan
Write-Host "  ================================================" -ForegroundColor DarkGray
Write-Host ""

python loader.py

Write-Host ""
Write-Host "  ================================================" -ForegroundColor DarkGray

if ($LASTEXITCODE -eq 0) {
    Write-Host "  ✓ Loader completed successfully!" -ForegroundColor Green
} else {
    Write-Host "  ✗ ERROR: Loader failed with exit code $LASTEXITCODE" -ForegroundColor Red
    Write-Host "  Check logs: $LOADER_DIR\loader.log" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Verification
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  Data Verification" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Database Statistics:" -ForegroundColor Yellow
mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE -e "
SELECT 
    'Authors' as Table_Name, 
    COUNT(*) as Record_Count,
    COUNT(DISTINCT author_id) as Unique_Records
FROM Authors
UNION ALL
SELECT 
    'Videos',
    COUNT(*),
    COUNT(DISTINCT video_id)
FROM Videos
UNION ALL
SELECT 
    'VideoInteractions',
    COUNT(*),
    COUNT(DISTINCT video_id)
FROM VideoInteractions
UNION ALL
SELECT 
    'RawJson',
    COUNT(*),
    COUNT(DISTINCT filename)
FROM RawJson
UNION ALL
SELECT 
    'LoadLog',
    COUNT(*),
    COUNT(DISTINCT batch_id)
FROM LoadLog;
"

Write-Host ""
Write-Host "Recent Load Logs:" -ForegroundColor Yellow
mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE -e "
SELECT 
    batch_id,
    table_name,
    record_count,
    inserted_count,
    updated_count,
    status,
    CONCAT(ROUND(duration_seconds, 2), 's') as duration
FROM LoadLog
ORDER BY created_at DESC
LIMIT 5;
"

Write-Host ""
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  ✓ Manual Run Completed Successfully!" -ForegroundColor Green
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  - View logs: Get-Content $LOADER_DIR\loader.log -Tail 50" -ForegroundColor Cyan
Write-Host "  - Check processed files: Get-ChildItem $STORAGE_DIR\processed" -ForegroundColor Cyan
Write-Host "  - Query data: mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE" -ForegroundColor Cyan
Write-Host ""
