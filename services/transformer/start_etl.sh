#!/bin/bash
set -e

echo "=== Waiting for MySQL to be ready ==="
sleep 10

echo "=== Dropping old procedure if exists ==="
mysql --skip-ssl --ssl=0 \
    -h "$MYSQL_HOST" -u"$MYSQL_ROOT_USER" -p"$MYSQL_ROOT_PASSWORD" "$DB_WAREHOUSE" \
    -e "DROP PROCEDURE IF EXISTS etl_tiktok_procedure;"

echo "=== Creating ETL Procedure ==="
mysql --skip-ssl --ssl=0 \
    -h "$MYSQL_HOST" -u"$MYSQL_ROOT_USER" -p"$MYSQL_ROOT_PASSWORD" "$DB_WAREHOUSE" \
    < /app/etl_transform.sql

echo "=== Running ETL ==="
echo "=== Running ETLáđâsđá ==="
echo "DB_WAREHOUSE = $DB_WAREHOUSE"
echo "MYSQL_HOST = $MYSQL_HOST"
echo "MYSQL_ROOT_USER = $MYSQL_ROOT_USER"

python3 /app/run_etl.py
