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

-- --------------------------------------------------
-- Staging schema for raw TikTok entities (Authors, Videos, Interactions)
-- --------------------------------------------------
CREATE DATABASE IF NOT EXISTS dbStaging;

-- DateDim table for date dimensions
CREATE TABLE IF NOT EXISTS dbStaging.DateDim (
    date_sk INT PRIMARY KEY,
    full_date VARCHAR(20),
    day_since_2005 INT,
    month_since_2005 INT,
    day_of_week VARCHAR(20),
    calendar_month VARCHAR(20),
    calendar_year VARCHAR(10),
    calendar_year_month VARCHAR(20),
    day_of_month INT,
    day_of_year INT,
    week_of_year_sunday INT,
    year_week_sunday VARCHAR(20),
    week_sunday_start VARCHAR(20),
    week_of_year_monday INT,
    year_week_monday VARCHAR(20),
    week_monday_start VARCHAR(20),
    holiday VARCHAR(50),
    day_type VARCHAR(20),
    KEY `idx_full_date` (`full_date`),
    KEY `idx_calendar_year` (`calendar_year`)
);

-- RawJson table for staging raw JSON data
CREATE TABLE IF NOT EXISTS dbStaging.RawJson (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    filename VARCHAR(255),
    content JSON,
    loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    load_status VARCHAR(50),
    error_message TEXT,
    source_line INT
);

-- Authors table with date dimension
CREATE TABLE IF NOT EXISTS dbStaging.Authors (
    authorID BIGINT PRIMARY KEY,
    Name VARCHAR(255),
    avatar TEXT,
    extract_date_sk INT,
    FOREIGN KEY (extract_date_sk) REFERENCES dbStaging.DateDim(date_sk)
);

-- Videos table with date dimension
CREATE TABLE IF NOT EXISTS dbStaging.Videos (
    videoID BIGINT PRIMARY KEY,
    authorID BIGINT,
    TextContent TEXT,
    Duration INT,
    CreateTime DATETIME,
    WebVideoUrl TEXT,
    create_date_sk INT,
    FOREIGN KEY (authorID) REFERENCES dbStaging.Authors(authorID),
    FOREIGN KEY (create_date_sk) REFERENCES dbStaging.DateDim(date_sk)
);

-- VideoInteractions table with date dimension
CREATE TABLE IF NOT EXISTS dbStaging.VideoInteractions (
    interactionID BIGINT AUTO_INCREMENT PRIMARY KEY,
    videoID BIGINT UNIQUE,
    DiggCount INT,
    PlayCount BIGINT,
    ShareCount INT,
    CommentCount INT,
    CollectCount INT,
    interaction_date_sk INT,
    FOREIGN KEY (videoID) REFERENCES dbStaging.Videos(videoID),
    FOREIGN KEY (interaction_date_sk) REFERENCES dbStaging.DateDim(date_sk)
);

-- LoadLog table for ETL audit trail
CREATE TABLE IF NOT EXISTS dbStaging.LoadLog (
    load_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(100),
    table_name VARCHAR(128),
    operation_type VARCHAR(50),
    record_count INT,
    status VARCHAR(50),
    start_time DATETIME,
    end_time DATETIME,
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY `idx_batch_id` (`batch_id`),
    KEY `idx_table_name` (`table_name`),
    KEY `idx_status` (`status`)
);
