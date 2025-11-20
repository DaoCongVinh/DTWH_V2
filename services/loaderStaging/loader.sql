-- Schema creation and data loading for staging database

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS dbStaging;

-- Create Authors table
CREATE TABLE IF NOT EXISTS dbStaging.Authors (
    authorID BIGINT PRIMARY KEY,
    Name VARCHAR(255),
    avatar TEXT
);

-- Create Videos table
CREATE TABLE IF NOT EXISTS dbStaging.Videos (
    videoID BIGINT PRIMARY KEY,
    authorID BIGINT,
    TextContent TEXT,
    Duration INT,
    CreateTime DATETIME,
    WebVideoUrl TEXT,
    FOREIGN KEY (authorID) REFERENCES dbStaging.Authors(authorID)
);

-- Create VideoInteractions table
CREATE TABLE IF NOT EXISTS dbStaging.VideoInteractions (
    interactionID BIGINT AUTO_INCREMENT PRIMARY KEY,
    videoID BIGINT UNIQUE,
    DiggCount INT,
    PlayCount BIGINT,
    ShareCount INT,
    CommentCount INT,
    CollectCount INT,
    FOREIGN KEY (videoID) REFERENCES dbStaging.Videos(videoID)
);

-- Create DateDim table for date dimension
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
    INDEX idx_full_date (full_date),
    INDEX idx_calendar_year (calendar_year)
);

-- ============================================================================
-- Raw JSON Storage Schema
-- ============================================================================

-- Create RawJson table to store raw JSON data
CREATE TABLE IF NOT EXISTS dbStaging.RawJson (
    id INT AUTO_INCREMENT PRIMARY KEY,
    filename VARCHAR(512) NOT NULL,
    content JSON NOT NULL,
    loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    load_status VARCHAR(50) DEFAULT 'success',
    error_message TEXT NULL,
    source_line INT NULL,
    INDEX idx_filename (filename),
    INDEX idx_loaded_at (loaded_at),
    INDEX idx_load_status (load_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
