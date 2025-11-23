# APIFY
APIFY_TOKEN=
APIFY_ACTOR=GdWCkxBtKWOsKjdch

# MySQL root (container)
MYSQL_ROOT_PASSWORD=rootpass

# CRAWLER DB (metadata)
CRAWLER_DB_USER=user
CRAWLER_DB_PASSWORD=dwhtiktok
CRAWLER_DATABASE=metadata_tiktok

# LOADER DB (staging)
LOADER_DB_USER=user
LOADER_DB_PASSWORD=dwhtiktok
LOADER_DATABASE=dbStaging

# Scheduler
SCHEDULE_CRON=*/1 * * * *
SCHEDULE_ENABLED=True

LOADER_SCHEDULE_CRON=0 */1 * * *
LOADER_SCHEDULE_ENABLED=True

# Email (for notifications)
MAIL_SENDER=tfmask2004@gmail.com
MAIL_PASSWORD=ihdhvxytgvcdfoau
MAIL_RECEIVER=khoapham2709@gmail.com

# ---------- MYSQL ROOT ----------
MYSQL_HOST=db
MYSQL_PORT=3306
MYSQL_ROOT_USER=root
MYSQL_ROOT_PASSWORD=rootpass

# ---------- DATABASES ----------
DB_METADATA=metadata_tiktok
DB_STAGING=dbStaging
DB_WAREHOUSE=warehouse_tiktok

# ---------- SCHEDULER ----------
LOADER_SCHEDULE_CRON=*/10 * * * *
LOADER_SCHEDULE_ENABLED=True