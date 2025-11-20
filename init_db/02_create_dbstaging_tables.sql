-- ===========================================
-- dbStaging Schema - Staging tables for raw TikTok data
-- ===========================================

USE dbStaging;

-- DateDim table for date dimensions
CREATE TABLE IF NOT EXISTS DateDim (
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
CREATE TABLE IF NOT EXISTS RawJson (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    filename VARCHAR(255),
    content JSON,
    loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    load_status VARCHAR(50),
    error_message TEXT,
    source_line INT
);

-- Authors table with date dimension
CREATE TABLE IF NOT EXISTS Authors (
    authorID BIGINT PRIMARY KEY,
    Name VARCHAR(255),
    avatar TEXT,
    extract_date_sk INT,
    FOREIGN KEY (extract_date_sk) REFERENCES DateDim(date_sk)
);

-- Videos table with date dimension
CREATE TABLE IF NOT EXISTS Videos (
    videoID BIGINT PRIMARY KEY,
    authorID BIGINT,
    TextContent TEXT,
    Duration INT,
    CreateTime DATETIME,
    WebVideoUrl TEXT,
    create_date_sk INT,
    FOREIGN KEY (authorID) REFERENCES Authors(authorID),
    FOREIGN KEY (create_date_sk) REFERENCES DateDim(date_sk)
);

-- VideoInteractions table with date dimension
CREATE TABLE IF NOT EXISTS VideoInteractions (
    interactionID BIGINT AUTO_INCREMENT PRIMARY KEY,
    videoID BIGINT UNIQUE,
    DiggCount INT,
    PlayCount BIGINT,
    ShareCount INT,
    CommentCount INT,
    CollectCount INT,
    interaction_date_sk INT,
    FOREIGN KEY (videoID) REFERENCES Videos(videoID),
    FOREIGN KEY (interaction_date_sk) REFERENCES DateDim(date_sk)
);

-- LoadLog table for ETL audit trail
CREATE TABLE IF NOT EXISTS LoadLog (
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
