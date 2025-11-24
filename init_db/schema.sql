CREATE DATABASE IF NOT EXISTS metadata_tiktok;
CREATE DATABASE IF NOT EXISTS staging_tiktok;
CREATE DATABASE IF NOT EXISTS warehouse_tiktok;
CREATE DATABASE IF NOT EXISTS dbStaging;
CREATE DATABASE IF NOT EXISTS dwh_tiktok;

-- Create user and grant permissions
CREATE USER IF NOT EXISTS 'user'@'%' IDENTIFIED BY 'dwhtiktok';
GRANT ALL PRIVILEGES ON dwh_tiktok.* TO 'user'@'%';
GRANT ALL PRIVILEGES ON dbStaging.* TO 'user'@'%';
GRANT ALL PRIVILEGES ON metadata_tiktok.* TO 'user'@'%';
GRANT ALL PRIVILEGES ON staging_tiktok.* TO 'user'@'%';
GRANT ALL PRIVILEGES ON warehouse_tiktok.* TO 'user'@'%';
FLUSH PRIVILEGES;

-- metadata
USE metadata_tiktok;

CREATE TABLE IF NOT EXISTS config_log (
    id_config INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(255),
    source_url VARCHAR(1024),
    api_endpoint VARCHAR(1024),
    file_path TEXT,
    file_pattern VARCHAR(255),
    date_format VARCHAR(64),
    schedule_time VARCHAR(64),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS control_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_config INT,
    file_name VARCHAR(255),
    status VARCHAR(64),
    extract_time DATETIME,
    total_record INT,
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (id_config) REFERENCES config_log(id_config)
);

-- staging
USE staging_tiktok;

CREATE TABLE IF NOT EXISTS staging_raw (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    device_id VARCHAR(128) NOT NULL,
    fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    apify_run_id VARCHAR(255) NULL,
    raw_json JSON NOT NULL,
    file_path VARCHAR(1024) NULL,
    processed BOOLEAN DEFAULT FALSE,
    UNIQUE KEY (apify_run_id, id)
);

-- warehouse
USE warehouse_tiktok;

CREATE TABLE IF NOT EXISTS dim_authors (
    authorID BIGINT PRIMARY KEY,
    authorName VARCHAR(255),
    avatarUrl TEXT,
    authorCategory VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_videos (
    videoID BIGINT PRIMARY KEY,
    authorID BIGINT,
    textContent TEXT,
    duration INT,
    createTime DATETIME,
    webVideoUrl TEXT,
    hashtagList TEXT,
    FOREIGN KEY (authorID) REFERENCES dim_authors(authorID)
);

CREATE TABLE IF NOT EXISTS dim_date (
    dateKey INT PRIMARY KEY,
    day VARCHAR(16),
    date DATE
);

CREATE TABLE IF NOT EXISTS fact_videos (
    interactionID BIGINT AUTO_INCREMENT PRIMARY KEY,
    videoID BIGINT,
    authorID BIGINT,
    dateKey INT,
    diggCount BIGINT,
    shareCount BIGINT,
    playCount BIGINT,
    commentCount BIGINT,
    collectCount BIGINT,
    createdAt DATETIME,
    FOREIGN KEY (videoID) REFERENCES dim_videos(videoID),
    FOREIGN KEY (authorID) REFERENCES dim_authors(authorID),
    FOREIGN KEY (dateKey) REFERENCES dim_date(dateKey)
);

CREATE INDEX idx_fact_videos_date ON fact_videos(dateKey);
CREATE INDEX idx_fact_videos_author ON fact_videos(authorID);
CREATE INDEX idx_dim_videos_author ON dim_videos(authorID);

-- ============================================================================
-- Schema: dbStaging - Loader Staging Database
-- Purpose: Store raw TikTok data, transform to 3 fact/dimension tables
-- Created: 2025-11-23
-- ============================================================================

-- Create database
CREATE DATABASE IF NOT EXISTS dbStaging CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE dbStaging;

-- ============================================================================
-- 1. RawJson Table - Store raw JSON from Crawler
-- ============================================================================
CREATE TABLE IF NOT EXISTS RawJson (
    raw_json_id INT AUTO_INCREMENT PRIMARY KEY,
    content LONGTEXT NOT NULL COMMENT 'Full JSON content from crawler',
    filename VARCHAR(255) NOT NULL COMMENT 'Source file name',
    load_status ENUM('SUCCESS', 'FAILED') DEFAULT 'SUCCESS' COMMENT 'Load result status',
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'When file was loaded',
    error_message TEXT COMMENT 'Error details if FAILED',
    INDEX idx_filename (filename),
    INDEX idx_load_status (load_status),
    INDEX idx_loaded_at (loaded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- 2. DateDim Table - Date dimension for SCD tracking
-- ============================================================================
CREATE TABLE IF NOT EXISTS DateDim (
    date_sk INT PRIMARY KEY COMMENT 'Surrogate key',
    full_date DATE NOT NULL UNIQUE COMMENT 'Full date (e.g., 2025-11-23)',
    day_since_2005 INT COMMENT 'Days since 2005-01-01',
    month_since_2005 INT COMMENT 'Months since 2005-01-01',
    day_of_week VARCHAR(10) COMMENT 'Day of week name',
    calendar_month VARCHAR(20) COMMENT 'Month name',
    calendar_year INT COMMENT 'Year',
    calendar_year_month VARCHAR(20) COMMENT 'Year-Month format',
    day_of_month INT COMMENT 'Day of month',
    day_of_year INT COMMENT 'Day of year',
    week_of_year_sunday INT COMMENT 'Week number (Sunday start)',
    year_week_sunday VARCHAR(10) COMMENT 'Year-Week format (Sunday)',
    week_sunday_start DATE COMMENT 'Week start date (Sunday)',
    week_of_year_monday INT COMMENT 'Week number (Monday start)',
    year_week_monday VARCHAR(10) COMMENT 'Year-Week format (Monday)',
    week_monday_start DATE COMMENT 'Week start date (Monday)',
    quarter VARCHAR(10) COMMENT 'Quarter (e.g., 2005-Q01)',
    month INT COMMENT 'Month number',
    holiday VARCHAR(50) COMMENT 'Holiday classification',
    day_type VARCHAR(20) COMMENT 'Day type (Weekday/Weekend)',
    INDEX idx_full_date (full_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- 3. Authors Table - Author dimension (SCD Type 2)
-- ============================================================================
CREATE TABLE IF NOT EXISTS Authors (
    author_id VARCHAR(50) NOT NULL,
    author_name VARCHAR(255) COMMENT 'Author/User name',
    avatar VARCHAR(1024) COMMENT 'Avatar URL',
    extract_date_sk INT NOT NULL COMMENT 'Date surrogate key (SCD)',
    is_current BOOLEAN DEFAULT TRUE COMMENT 'Is current record (SCD)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (author_id, extract_date_sk),
    FOREIGN KEY (extract_date_sk) REFERENCES DateDim(date_sk),
    INDEX idx_author_id (author_id),
    INDEX idx_extract_date_sk (extract_date_sk),
    INDEX idx_is_current (is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- 4. Videos Table - Video dimension (SCD Type 2)
-- ============================================================================
CREATE TABLE IF NOT EXISTS Videos (
    video_id VARCHAR(50) NOT NULL,
    author_id VARCHAR(50) NOT NULL,
    text_content TEXT COMMENT 'Video caption/text',
    duration INT COMMENT 'Duration in seconds',
    create_time DATETIME COMMENT 'Video creation timestamp',
    web_video_url VARCHAR(1024) COMMENT 'TikTok URL',
    create_date_sk INT NOT NULL COMMENT 'Date surrogate key (SCD)',
    is_current BOOLEAN DEFAULT TRUE COMMENT 'Is current record (SCD)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (video_id, create_date_sk),
    FOREIGN KEY (author_id) REFERENCES Authors(author_id),
    FOREIGN KEY (create_date_sk) REFERENCES DateDim(date_sk),
    INDEX idx_video_id (video_id),
    INDEX idx_author_id (author_id),
    INDEX idx_create_date_sk (create_date_sk),
    INDEX idx_is_current (is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- 5. VideoInteractions Table - Fact table for video stats (SCD Type 2)
-- ============================================================================
CREATE TABLE IF NOT EXISTS VideoInteractions (
    interaction_id INT AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(50) NOT NULL,
    digg_count INT DEFAULT 0 COMMENT 'Like count',
    play_count INT DEFAULT 0 COMMENT 'View count',
    share_count INT DEFAULT 0 COMMENT 'Share count',
    comment_count INT DEFAULT 0 COMMENT 'Comment count',
    collect_count INT DEFAULT 0 COMMENT 'Save/Collect count',
    interaction_date_sk INT NOT NULL COMMENT 'Date surrogate key (SCD)',
    is_current BOOLEAN DEFAULT TRUE COMMENT 'Is current record (SCD)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_interaction (video_id, interaction_date_sk),
    FOREIGN KEY (video_id) REFERENCES Videos(video_id),
    FOREIGN KEY (interaction_date_sk) REFERENCES DateDim(date_sk),
    INDEX idx_video_id (video_id),
    INDEX idx_interaction_date_sk (interaction_date_sk),
    INDEX idx_is_current (is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- 6. LoadLog Table - Audit trail for each load operation
-- ============================================================================
CREATE TABLE IF NOT EXISTS LoadLog (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(100) NOT NULL COMMENT 'Batch ID for grouping',
    table_name VARCHAR(50) NOT NULL COMMENT 'Target table name',
    record_count INT DEFAULT 0 COMMENT 'Number of records processed',
    inserted_count INT DEFAULT 0 COMMENT 'Number of records inserted',
    updated_count INT DEFAULT 0 COMMENT 'Number of records updated',
    skipped_count INT DEFAULT 0 COMMENT 'Number of records skipped',
    status ENUM('SUCCESS', 'FAILED', 'PARTIAL') DEFAULT 'SUCCESS',
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration_seconds DECIMAL(10,2) COMMENT 'Execution time',
    source_filename VARCHAR(255) COMMENT 'Source JSON file name',
    error_message TEXT COMMENT 'Error details if FAILED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_batch_id (batch_id),
    INDEX idx_table_name (table_name),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
