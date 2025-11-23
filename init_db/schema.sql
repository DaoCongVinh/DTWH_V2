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

CREATE TABLE etl_run_log (
    run_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    
    procedure_name VARCHAR(255),
    start_time DATETIME NOT NULL,
    end_time DATETIME DEFAULT NULL,

    status ENUM('RUNNING', 'SUCCESS', 'FAILED') NOT NULL,

    -- Performance metrics
    inserted_dim_authors INT DEFAULT 0,
    updated_dim_authors INT DEFAULT 0,
    inserted_dim_videos INT DEFAULT 0,
    updated_dim_videos INT DEFAULT 0,
    inserted_fact INT DEFAULT 0,
    updated_fact INT DEFAULT 0,

    -- Error tracking
    error_message TEXT DEFAULT NULL,
    error_state VARCHAR(10) DEFAULT NULL,
    
    -- System / debugging
    host_name VARCHAR(255) DEFAULT NULL,
    container_id VARCHAR(255) DEFAULT NULL,
    procedure_version VARCHAR(20) DEFAULT NULL,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
-- ============================================================================
-- Create warehouse database
-- ============================================================================
CREATE DATABASE IF NOT EXISTS warehouse_tiktok
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE warehouse_tiktok;

-- ============================================================================
-- dim_authors
-- ============================================================================
CREATE TABLE IF NOT EXISTS dim_authors (
    author_sk INT AUTO_INCREMENT PRIMARY KEY,
    author_id VARCHAR(50) NOT NULL,
    author_name VARCHAR(255),
    avatar VARCHAR(1024),

    start_date_sk INT NOT NULL,
    end_date_sk INT DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_author_id (author_id),
    INDEX idx_is_current (is_current),

    FOREIGN KEY (start_date_sk) REFERENCES dbStaging.DateDim(date_sk),
    FOREIGN KEY (end_date_sk)   REFERENCES dbStaging.DateDim(date_sk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ============================================================================
-- dim_videos
-- ============================================================================
CREATE TABLE IF NOT EXISTS dim_videos (
    video_sk INT AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(50) NOT NULL,
    author_id VARCHAR(50) NOT NULL,

    text_content TEXT,
    duration INT,
    create_time DATETIME,
    web_video_url VARCHAR(1024),

    start_date_sk INT NOT NULL,
    end_date_sk INT DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_video_id (video_id),
    INDEX idx_author_id (author_id),
    INDEX idx_is_current (is_current),

    FOREIGN KEY (start_date_sk) REFERENCES dbStaging.DateDim(date_sk),
    FOREIGN KEY (end_date_sk)   REFERENCES dbStaging.DateDim(date_sk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ============================================================================
-- fact_video_interactions
-- ============================================================================
CREATE TABLE IF NOT EXISTS fact_video_interactions (
    interaction_sk INT AUTO_INCREMENT PRIMARY KEY,
    video_id VARCHAR(50) NOT NULL,

    digg_count INT DEFAULT 0,
    play_count INT DEFAULT 0,
    share_count INT DEFAULT 0,
    comment_count INT DEFAULT 0,
    collect_count INT DEFAULT 0,

    start_date_sk INT NOT NULL,
    end_date_sk INT DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_video_id (video_id),
    INDEX idx_is_current (is_current),

    FOREIGN KEY (start_date_sk) REFERENCES dbStaging.DateDim(date_sk),
    FOREIGN KEY (end_date_sk)   REFERENCES dbStaging.DateDim(date_sk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;




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
