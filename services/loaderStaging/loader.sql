-- Schema creation and transformation procedures for dbStaging

CREATE DATABASE IF NOT EXISTS dbStaging;
USE dbStaging;

-- ============================================================================
-- Dimension & staging tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS Authors (
    authorID BIGINT PRIMARY KEY,
    Name VARCHAR(255),
    avatar TEXT,
    extract_date_sk INT,
    INDEX idx_author_extract_date (extract_date_sk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS Videos (
    videoID BIGINT PRIMARY KEY,
    authorID BIGINT,
    TextContent TEXT,
    Duration INT,
    CreateTime DATETIME,
    WebVideoUrl TEXT,
    create_date_sk INT,
    FOREIGN KEY (authorID) REFERENCES Authors(authorID),
    INDEX idx_video_create_date (create_date_sk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
    INDEX idx_interaction_date (interaction_date_sk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
    quarter INT,
    quarter_raw VARCHAR(20),
    month_num INT,
    holiday VARCHAR(50),
    day_type VARCHAR(20),
    INDEX idx_full_date (full_date),
    INDEX idx_calendar_year (calendar_year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS DateDimStaging (
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
    quarter INT,
    quarter_raw VARCHAR(20),
    month_num INT,
    holiday VARCHAR(50),
    day_type VARCHAR(20),
    INDEX idx_full_date (full_date),
    INDEX idx_calendar_year (calendar_year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS RawJson (
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

CREATE TABLE IF NOT EXISTS LoadLog (
    id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(128),
    table_name VARCHAR(128),
    operation_type VARCHAR(64),
    record_count INT DEFAULT 0,
    status VARCHAR(32),
    start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    end_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT,
    INDEX idx_batch (batch_id),
    INDEX idx_table_name (table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================================
-- Reusable procedures
-- ============================================================================

DROP PROCEDURE IF EXISTS insert_load_log;
CREATE PROCEDURE insert_load_log(
    IN p_batch_id VARCHAR(128),
    IN p_table_name VARCHAR(128),
    IN p_operation VARCHAR(64),
    IN p_record_count INT,
    IN p_status VARCHAR(32),
    IN p_error_message TEXT
)
BEGIN
    INSERT INTO LoadLog (
        batch_id, table_name, operation_type, record_count, status, start_time, end_time, error_message
    ) VALUES (
        p_batch_id,
        p_table_name,
        p_operation,
        p_record_count,
        p_status,
        NOW(),
        NOW(),
        p_error_message
    );
END;

DROP PROCEDURE IF EXISTS process_raw_record;
CREATE PROCEDURE process_raw_record(
    IN p_filename VARCHAR(512),
    IN p_payload JSON,
    IN p_source_line INT
)
BEGIN
    DECLARE v_video_id VARCHAR(128);
    DECLARE v_author_id VARCHAR(128);
    DECLARE v_author_name VARCHAR(255);
    DECLARE v_avatar TEXT;
    DECLARE v_text TEXT;
    DECLARE v_duration INT DEFAULT 0;
    DECLARE v_create_time DATETIME;
    DECLARE v_date_sk INT;
    DECLARE v_web_url TEXT;
    DECLARE v_digg BIGINT DEFAULT 0;
    DECLARE v_play BIGINT DEFAULT 0;
    DECLARE v_share BIGINT DEFAULT 0;
    DECLARE v_comment BIGINT DEFAULT 0;
    DECLARE v_collect BIGINT DEFAULT 0;
    DECLARE v_raw_ts VARCHAR(128);

    proc_block: BEGIN
        SET v_video_id = COALESCE(
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.id')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.video.id')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.itemInfo.itemId'))
        );

        IF v_video_id IS NULL OR v_video_id = '' THEN
            LEAVE proc_block;
        END IF;

        SET v_author_id = COALESCE(
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.authorMeta.id')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.author.id')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.author.secUid')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.author.uniqueId'))
        );

        SET v_author_name = COALESCE(
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.authorMeta.name')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.authorMeta.uniqueId')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.author.uniqueId'))
        );

        SET v_avatar = COALESCE(
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.authorMeta.avatar')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.author.avatarThumb')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.author.avatarMedium')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.author.avatarLarger'))
        );

        SET v_text = COALESCE(
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.text')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.desc')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.itemInfo.text'))
        );

        SET v_duration = COALESCE(
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.duration')) AS UNSIGNED),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.video.duration')) AS UNSIGNED),
            0
        );

        SET v_raw_ts = COALESCE(
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.createTime')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.itemInfo.createTime'))
        );

        IF v_raw_ts IS NOT NULL AND v_raw_ts <> '' THEN
            SET v_raw_ts = REPLACE(REPLACE(v_raw_ts, 'T', ' '), 'Z', '');
            IF v_raw_ts REGEXP '^[0-9]+$' THEN
                SET v_create_time = FROM_UNIXTIME(CAST(v_raw_ts AS UNSIGNED));
            ELSE
                SET v_create_time = STR_TO_DATE(v_raw_ts, '%Y-%m-%d %H:%i:%s');
                IF v_create_time IS NULL THEN
                    SET v_create_time = STR_TO_DATE(SUBSTRING(v_raw_ts, 1, 19), '%Y-%m-%d %H:%i:%s');
                END IF;
            END IF;
        END IF;

        IF v_create_time IS NOT NULL THEN
            SELECT date_sk INTO v_date_sk
            FROM DateDim
            WHERE full_date LIKE CONCAT(DATE_FORMAT(v_create_time, '%Y-%m-%d'), '%')
            LIMIT 1;
        END IF;

        SET v_web_url = COALESCE(
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.webVideoUrl')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.shareUrl')),
            JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.url'))
        );

        SET v_digg = COALESCE(
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.diggCount')) AS UNSIGNED),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.stats.diggCount')) AS UNSIGNED),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.statistics.likeCount')) AS UNSIGNED),
            0
        );

        SET v_play = COALESCE(
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.playCount')) AS UNSIGNED),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.stats.playCount')) AS UNSIGNED),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.stats.playCountSum')) AS UNSIGNED),
            0
        );

        SET v_share = COALESCE(
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.shareCount')) AS UNSIGNED),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.stats.shareCount')) AS UNSIGNED),
            0
        );

        SET v_comment = COALESCE(
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.commentCount')) AS UNSIGNED),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.stats.commentCount')) AS UNSIGNED),
            0
        );

        SET v_collect = COALESCE(
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.collectCount')) AS UNSIGNED),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.stats.collectCount')) AS UNSIGNED),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(p_payload, '$.stats.saveCount')) AS UNSIGNED),
            0
        );

        INSERT INTO RawJson (filename, content, load_status, source_line)
        VALUES (p_filename, p_payload, 'processed', p_source_line);

        IF v_author_id IS NOT NULL AND v_author_id <> '' THEN
            INSERT INTO Authors (authorID, Name, avatar, extract_date_sk)
            VALUES (CAST(v_author_id AS UNSIGNED), v_author_name, v_avatar, v_date_sk)
            ON DUPLICATE KEY UPDATE
                Name = VALUES(Name),
                avatar = VALUES(avatar),
                extract_date_sk = VALUES(extract_date_sk);
        END IF;

        INSERT INTO Videos (
            videoID, authorID, TextContent, Duration, CreateTime, WebVideoUrl, create_date_sk
        ) VALUES (
            CAST(v_video_id AS UNSIGNED),
            NULLIF(CAST(v_author_id AS UNSIGNED), 0),
            v_text,
            v_duration,
            v_create_time,
            v_web_url,
            v_date_sk
        )
        ON DUPLICATE KEY UPDATE
            authorID = VALUES(authorID),
            TextContent = VALUES(TextContent),
            Duration = VALUES(Duration),
            CreateTime = VALUES(CreateTime),
            WebVideoUrl = VALUES(WebVideoUrl),
            create_date_sk = VALUES(create_date_sk);

        INSERT INTO VideoInteractions (
            videoID, DiggCount, PlayCount, ShareCount, CommentCount, CollectCount, interaction_date_sk
        ) VALUES (
            CAST(v_video_id AS UNSIGNED),
            v_digg,
            v_play,
            v_share,
            v_comment,
            v_collect,
            v_date_sk
        )
        ON DUPLICATE KEY UPDATE
            DiggCount = VALUES(DiggCount),
            PlayCount = VALUES(PlayCount),
            ShareCount = VALUES(ShareCount),
            CommentCount = VALUES(CommentCount),
            CollectCount = VALUES(CollectCount),
            interaction_date_sk = VALUES(interaction_date_sk);
    END proc_block;
END;

DROP PROCEDURE IF EXISTS load_date_dim_from_csv;
CREATE PROCEDURE load_date_dim_from_csv(
    IN p_csv_path VARCHAR(512)
)
BEGIN
    DECLARE v_sql TEXT;
    DECLARE v_escaped VARCHAR(1024);
    DECLARE v_batch VARCHAR(64);
    DECLARE v_col_count INT DEFAULT 0;

    SET v_batch = CONCAT('date_dim_load_', DATE_FORMAT(NOW(), '%Y%m%d_%H%i%s'));
    TRUNCATE TABLE DateDimStaging;
    
    -- Check if quarter_raw column exists, add if not
    SELECT COUNT(*) INTO v_col_count
    FROM information_schema.COLUMNS 
    WHERE TABLE_SCHEMA = DATABASE() 
      AND TABLE_NAME = 'DateDimStaging' 
      AND COLUMN_NAME = 'quarter_raw';
    
    IF v_col_count = 0 THEN
        ALTER TABLE DateDimStaging ADD COLUMN quarter_raw VARCHAR(20) AFTER quarter;
    END IF;
    
    SET v_escaped = REPLACE(p_csv_path, '\\', '\\\\');
    SET v_sql = CONCAT(
        "LOAD DATA LOCAL INFILE '",
        v_escaped,
        "' INTO TABLE DateDimStaging FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' ",
        "(date_sk, full_date, day_since_2005, month_since_2005, day_of_week, calendar_month, calendar_year, calendar_year_month, ",
        "day_of_month, day_of_year, week_of_year_sunday, year_week_sunday, week_sunday_start, week_of_year_monday, ",
        "year_week_monday, week_monday_start, quarter_raw, month_num, holiday, day_type)"
    );
    PREPARE stmt FROM v_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Convert quarter_raw to quarter integer
    UPDATE DateDimStaging 
    SET quarter = CASE 
        WHEN quarter_raw LIKE '%Q01' THEN 1
        WHEN quarter_raw LIKE '%Q02' THEN 2
        WHEN quarter_raw LIKE '%Q03' THEN 3
        WHEN quarter_raw LIKE '%Q04' THEN 4
        ELSE CAST(quarter_raw AS UNSIGNED)
    END;

    INSERT INTO DateDim (
        date_sk, full_date, day_since_2005, month_since_2005,
        day_of_week, calendar_month, calendar_year, calendar_year_month,
        day_of_month, day_of_year, week_of_year_sunday, year_week_sunday,
        week_sunday_start, week_of_year_monday, year_week_monday,
        week_monday_start, quarter, quarter_raw, month_num, holiday, day_type
    )
    SELECT
        date_sk, full_date, day_since_2005, month_since_2005,
        day_of_week, calendar_month, calendar_year, calendar_year_month,
        day_of_month, day_of_year, week_of_year_sunday, year_week_sunday,
        week_sunday_start, week_of_year_monday, year_week_monday,
        week_monday_start, quarter, quarter_raw, month_num, holiday, day_type
    FROM DateDimStaging
    ON DUPLICATE KEY UPDATE
        full_date = VALUES(full_date),
        day_since_2005 = VALUES(day_since_2005),
        month_since_2005 = VALUES(month_since_2005),
        day_of_week = VALUES(day_of_week),
        calendar_month = VALUES(calendar_month),
        calendar_year = VALUES(calendar_year),
        calendar_year_month = VALUES(calendar_year_month),
        day_of_month = VALUES(day_of_month),
        day_of_year = VALUES(day_of_year),
        week_of_year_sunday = VALUES(week_of_year_sunday),
        year_week_sunday = VALUES(year_week_sunday),
        week_sunday_start = VALUES(week_sunday_start),
        week_of_year_monday = VALUES(week_of_year_monday),
        year_week_monday = VALUES(year_week_monday),
        week_monday_start = VALUES(week_monday_start),
        quarter = VALUES(quarter),
        quarter_raw = VALUES(quarter_raw),
        month_num = VALUES(month_num),
        holiday = VALUES(holiday),
        day_type = VALUES(day_type);
        
    -- Clean up temporary column from staging only
    SELECT COUNT(*) INTO v_col_count
    FROM information_schema.COLUMNS 
    WHERE TABLE_SCHEMA = DATABASE() 
      AND TABLE_NAME = 'DateDimStaging' 
      AND COLUMN_NAME = 'quarter_raw';
    
    IF v_col_count > 0 THEN
        ALTER TABLE DateDimStaging DROP COLUMN quarter_raw;
    END IF;

    CALL insert_load_log(v_batch, 'DateDim', 'UPSERT', (SELECT COUNT(*) FROM DateDimStaging), 'success', NULL);
END;
