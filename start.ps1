#!/usr/bin/env pwsh
# Script to start the entire DTWH_V2 project from scratch

Write-Host "🚀 Starting DTWH_V2 Project..." -ForegroundColor Cyan
Write-Host ""

# Step 1: Stop and remove existing containers and volumes
Write-Host "📦 Stopping existing containers..." -ForegroundColor Yellow
docker compose down -v
Write-Host ""

# Step 2: Clean Docker build cache for loader-staging
Write-Host "🧹 Cleaning Docker cache..." -ForegroundColor Yellow
docker builder prune -f
Write-Host ""

# Step 3: Build all services
Write-Host "🔨 Building services..." -ForegroundColor Yellow
docker compose build
Write-Host ""

# Step 4: Start database first
Write-Host "🗄️  Starting database..." -ForegroundColor Yellow
docker compose up -d db
Write-Host ""

# Step 5: Wait for database to be healthy and init scripts to complete
Write-Host "⏳ Waiting for database initialization..." -ForegroundColor Yellow
$maxRetries = 5
$retryCount = 0
$tableExists = $false

while ($retryCount -lt $maxRetries) {
    $retryCount++
    
    # Check if database is healthy first
    $healthStatus = docker compose ps db --format json 2>$null | ConvertFrom-Json | Select-Object -ExpandProperty Health
    
    if ($healthStatus -eq "healthy") {
        # Now check if DateDim table exists (init scripts completed)
        $tableCheck = docker compose exec -T db mysql -h 127.0.0.1 -uroot -p$env:MYSQL_ROOT_PASSWORD -sN -e "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbStaging' AND TABLE_NAME='DateDim';" 2>$null
        
        if ($tableCheck -match "^\s*1\s*$") {
            $tableExists = $true
            Write-Host "✓ Database is ready and DateDim table exists!" -ForegroundColor Green
            break
        }
    }
    
    Write-Host "Waiting for database and init scripts... (attempt $retryCount/$maxRetries)" -ForegroundColor Gray
    Start-Sleep -Seconds 2
}

if (-not $tableExists) {
    Write-Host "✗ ERROR: Database initialization failed" -ForegroundColor Red
    Write-Host "Checking database logs..." -ForegroundColor Yellow
    docker compose logs db --tail 100
    exit 1
}
Write-Host ""

# Step 6: Start loader-staging (will auto-load DateDim data)
Write-Host "📥 Starting loader-staging..." -ForegroundColor Yellow
docker compose up -d loader-staging
Write-Host ""

# Step 7: Wait and check loader logs
Write-Host "⏳ Waiting for loader to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 15
Write-Host ""

Write-Host "📋 Loader-staging logs:" -ForegroundColor Cyan
docker compose logs loader-staging --tail 30
Write-Host ""

# Step 8: Start crawler
Write-Host "🕷️  Starting crawler..." -ForegroundColor Yellow
docker compose up -d crawler
Write-Host ""

# Final status
Write-Host "✅ Project started successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "📊 Container status:" -ForegroundColor Cyan
docker compose ps
Write-Host ""

Write-Host "� Useful colmmands:" -ForegroundColor Yellow
Write-Host "  - View all logs: docker compose logs -f"
Write-Host "  - View loader logs: docker compose logs -f loader-staging"
Write-Host "  - View crawler logs: docker compose logs -f crawler"
Write-Host "  - Stop all: docker compose down"
Write-Host "  - Restart loader: docker compose restart loader-staging"
