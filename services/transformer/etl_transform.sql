DELIMITER $$

CREATE PROCEDURE etl_tiktok_procedure()
BEGIN
    /* =========================
       Common variables
       ========================= */
    DECLARE v_today_sk INT;
    DECLARE v_run_id   BIGINT;
    DECLARE v_cnt      INT DEFAULT 0;

    DECLARE v_ins_auth   INT DEFAULT 0;
    DECLARE v_upd_auth   INT DEFAULT 0;
    DECLARE v_ins_videos INT DEFAULT 0;
    DECLARE v_upd_videos INT DEFAULT 0;
    DECLARE v_ins_fact   INT DEFAULT 0;
    DECLARE v_upd_fact   INT DEFAULT 0;

    DECLARE v_error_message TEXT;
    DECLARE v_error_state   CHAR(5);


    /* =========================
       Exception handler
       ========================= */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        BEGIN
            DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;
            GET DIAGNOSTICS CONDITION 1
                v_error_state   = RETURNED_SQLSTATE,
                v_error_message = MESSAGE_TEXT;
        END;

        ROLLBACK;

        IF v_run_id IS NOT NULL THEN
            UPDATE metadata_tiktok.etl_run_log
            SET end_time      = NOW(),
                status        = 'FAILED',
                error_message = CONCAT('[SQLSTATE ', v_error_state, '] ', v_error_message)
            WHERE run_id = v_run_id;
        END IF;
    END;


    /* =========================
       Insert ETL start log
       ========================= */
    INSERT INTO metadata_tiktok.etl_run_log (procedure_name, start_time, status)
    VALUES ('etl_tiktok_procedure', NOW(), 'RUNNING');

    SET v_run_id = LAST_INSERT_ID();


    /* =========================
       Start transaction
       ========================= */
    START TRANSACTION;


    /* ============================================================
       STAGING VALIDATIONS (ENGLISH LOGGING)
       ============================================================ */

    -- 1) Check DateDim contains today's date
    SELECT COUNT(*) INTO v_cnt
    FROM dbStaging.DateDim
    WHERE full_date = CURRENT_DATE();

    IF v_cnt = 0 THEN
        SIGNAL SQLSTATE '45001'
            SET MESSAGE_TEXT = 'STAGING ERROR: Missing today''s row in dbStaging.DateDim.';
    END IF;

    SELECT date_sk INTO v_today_sk
    FROM dbStaging.DateDim
    WHERE full_date = CURRENT_DATE()
    LIMIT 1;


    -- 2) Check Authors has data
    SELECT COUNT(*) INTO v_cnt
    FROM dbStaging.Authors;

    IF v_cnt = 0 THEN
        SIGNAL SQLSTATE '45002'
            SET MESSAGE_TEXT = 'STAGING ERROR: dbStaging.Authors is empty.';
    END IF;


    -- 3) Check Videos has data
    SELECT COUNT(*) INTO v_cnt
    FROM dbStaging.Videos;

    IF v_cnt = 0 THEN
        SIGNAL SQLSTATE '45003'
            SET MESSAGE_TEXT = 'STAGING ERROR: dbStaging.Videos is empty.';
    END IF;


    -- 4) Check VideoInteractions has data
    SELECT COUNT(*) INTO v_cnt
    FROM dbStaging.VideoInteractions;

    IF v_cnt = 0 THEN
        SIGNAL SQLSTATE '45004'
            SET MESSAGE_TEXT = 'STAGING ERROR: dbStaging.VideoInteractions is empty.';
    END IF;



    /* ============================================================
       ================== DIM AUTHORS (SCD2) ======================
       ============================================================ */

    UPDATE warehouse_tiktok.dim_authors d
    JOIN dbStaging.Authors s
        ON d.author_id = s.author_id AND d.is_current = 1
    SET d.is_current  = 0,
        d.end_date_sk = v_today_sk
    WHERE COALESCE(d.author_name, '') <> COALESCE(s.author_name, '')
       OR COALESCE(d.avatar, '')      <> COALESCE(s.avatar, '');

    SET v_upd_auth = ROW_COUNT();


    INSERT INTO warehouse_tiktok.dim_authors (
        author_id, author_name, avatar,
        start_date_sk, end_date_sk, is_current
    )
    SELECT 
        s.author_id, s.author_name, s.avatar,
        v_today_sk, NULL, 1
    FROM dbStaging.Authors s
    LEFT JOIN warehouse_tiktok.dim_authors d 
        ON s.author_id = d.author_id AND d.is_current = 1
    WHERE d.author_id IS NULL
       OR COALESCE(d.author_name, '') <> COALESCE(s.author_name, '')
       OR COALESCE(d.avatar, '')      <> COALESCE(s.avatar, '');

    SET v_ins_auth = ROW_COUNT();



    /* ============================================================
       ================== DIM VIDEOS (SCD2) ======================
       ============================================================ */

    UPDATE warehouse_tiktok.dim_videos d
    JOIN dbStaging.Videos s
        ON d.video_id = s.video_id AND d.is_current = 1
    SET d.is_current  = 0,
        d.end_date_sk = v_today_sk
    WHERE COALESCE(d.text_content, '') <> COALESCE(s.text_content, '')
       OR COALESCE(d.duration, 0)      <> COALESCE(s.duration, 0)
       OR COALESCE(d.web_video_url, '') <> COALESCE(s.web_video_url, '');

    SET v_upd_videos = ROW_COUNT();


    INSERT INTO warehouse_tiktok.dim_videos (
        video_id, author_id, text_content, duration,
        create_time, web_video_url,
        start_date_sk, end_date_sk, is_current
    )
    SELECT
        s.video_id, s.author_id, s.text_content, s.duration,
        s.create_time, s.web_video_url,
        v_today_sk, NULL, 1
    FROM dbStaging.Videos s
    LEFT JOIN warehouse_tiktok.dim_videos d 
        ON s.video_id = d.video_id AND d.is_current = 1
    WHERE d.video_id IS NULL
       OR COALESCE(d.text_content, '') <> COALESCE(s.text_content, '')
       OR COALESCE(d.duration, 0)      <> COALESCE(s.duration, 0)
       OR COALESCE(d.web_video_url, '') <> COALESCE(s.web_video_url, '');

    SET v_ins_videos = ROW_COUNT();



    /* ============================================================
       ========= FACT VIDEO INTERACTIONS (SCD2) ===================
       ============================================================ */

    UPDATE warehouse_tiktok.fact_video_interactions f
    JOIN dbStaging.VideoInteractions s
        ON f.video_id = s.video_id AND f.is_current = 1
    SET f.is_current  = 0,
        f.end_date_sk = v_today_sk
    WHERE COALESCE(f.digg_count, 0)    <> COALESCE(s.digg_count, 0)
       OR COALESCE(f.play_count, 0)    <> COALESCE(s.play_count, 0)
       OR COALESCE(f.share_count, 0)   <> COALESCE(s.share_count, 0)
       OR COALESCE(f.comment_count, 0) <> COALESCE(s.comment_count, 0)
       OR COALESCE(f.collect_count, 0) <> COALESCE(s.collect_count, 0);

    SET v_upd_fact = ROW_COUNT();


    INSERT INTO warehouse_tiktok.fact_video_interactions (
        video_id, digg_count, play_count,
        share_count, comment_count, collect_count,
        start_date_sk, end_date_sk, is_current
    )
    SELECT
        s.video_id, s.digg_count, s.play_count,
        s.share_count, s.comment_count, s.collect_count,
        v_today_sk, NULL, 1
    FROM dbStaging.VideoInteractions s
    LEFT JOIN warehouse_tiktok.fact_video_interactions f
        ON s.video_id = f.video_id AND f.is_current = 1
    WHERE f.video_id IS NULL
       OR COALESCE(f.digg_count, 0)    <> COALESCE(s.digg_count, 0)
       OR COALESCE(f.play_count, 0)    <> COALESCE(s.play_count, 0)
       OR COALESCE(f.share_count, 0)   <> COALESCE(s.share_count, 0)
       OR COALESCE(f.comment_count, 0) <> COALESCE(s.comment_count, 0)
       OR COALESCE(f.collect_count, 0) <> COALESCE(s.collect_count, 0);

    SET v_ins_fact = ROW_COUNT();



    /* =========================
       Commit + update log
       ========================= */
    COMMIT;

    UPDATE metadata_tiktok.etl_run_log
    SET end_time            = NOW(),
        status              = 'SUCCESS',
        inserted_dim_authors = v_ins_auth,
        updated_dim_authors  = v_upd_auth,
        inserted_dim_videos  = v_ins_videos,
        updated_dim_videos   = v_upd_videos,
        inserted_fact        = v_ins_fact,
        updated_fact         = v_upd_fact
    WHERE run_id = v_run_id;

END$$

DELIMITER ;
