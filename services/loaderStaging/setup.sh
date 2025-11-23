#!/bin/bash
# Setup script for Loader Staging

set -e

echo "=================================="
echo "TikTok Loader Staging Setup"
echo "=================================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if .env exists
if [ ! -f .env ]; then
    echo -e "${RED}Error: .env file not found${NC}"
    echo "Please create .env file with required variables"
    exit 1
fi

echo -e "${YELLOW}Step 1: Creating directories...${NC}"
mkdir -p storage/processed storage/failed logs
echo -e "${GREEN}✓ Directories created${NC}"

echo -e "${YELLOW}Step 2: Building Docker image...${NC}"
docker build -t dtwh-loader-staging ./services/loaderStaging
echo -e "${GREEN}✓ Image built${NC}"

echo -e "${YELLOW}Step 3: Starting services...${NC}"
docker-compose up -d db
echo "Waiting for database to be ready..."
sleep 10
docker-compose ps
echo -e "${GREEN}✓ Services started${NC}"

echo -e "${YELLOW}Step 4: Creating database schema...${NC}"
docker-compose exec -T db mysql -u root -p"${MYSQL_ROOT_PASSWORD}" < services/loaderStaging/schema_dbStaging.sql
echo -e "${GREEN}✓ Schema created${NC}"

echo -e "${YELLOW}Step 5: Loading DateDim data...${NC}"
docker-compose exec -T db mysql -u root -p"${MYSQL_ROOT_PASSWORD}" dbStaging -e "
LOAD DATA LOCAL INFILE '/docker-entrypoint-initdb.d/date_dim.csv'
INTO TABLE DateDim
FIELDS TERMINATED BY ','
ENCLOSED BY '\"'
LINES TERMINATED BY '\\n'
IGNORE 1 ROWS
(date_sk, full_date, year, month, day);
"
echo -e "${GREEN}✓ DateDim loaded${NC}"

echo -e "${YELLOW}Step 6: Verifying database...${NC}"
docker-compose exec -T db mysql -u root -p"${MYSQL_ROOT_PASSWORD}" dbStaging -e "
SELECT COUNT(*) as date_records FROM DateDim;
SELECT COUNT(*) as raw_json_records FROM RawJson;
"
echo -e "${GREEN}✓ Database verified${NC}"

echo -e "${YELLOW}Step 7: Starting loader service...${NC}"
docker-compose up -d loader-staging
sleep 5
docker-compose logs loader-staging
echo -e "${GREEN}✓ Loader service started${NC}"

echo ""
echo "=================================="
echo -e "${GREEN}Setup completed successfully!${NC}"
echo "=================================="
echo ""
echo "Useful commands:"
echo "  docker-compose up -d loader-staging    # Start loader"
echo "  docker-compose logs -f loader-staging  # View logs"
echo "  docker-compose ps                      # Check services"
echo "  docker-compose down -v                 # Stop everything"
echo ""
echo "Check status:"
docker-compose ps
