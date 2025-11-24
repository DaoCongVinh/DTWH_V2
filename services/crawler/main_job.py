import os
import json
from datetime import datetime
import config
from logging_setup import logger
import db
import apify_service
import notification

def job():
    """Pipeline chính: Lỗi ở bất kỳ đâu cũng sẽ gửi mail."""
    id_config = None 
    current_step = "Init"

    try:
        # --- BƯỚC 1: KẾT NỐI DB & CONFIG ---
        current_step = "Connect Database"
        logger.info("Job started. Connecting to DB...")
        
        # Nếu DB chết, hàm này sẽ văng lỗi ngay -> Nhảy xuống except -> Gửi mail
        id_config = db.save_config_to_db() 
        
        # --- BƯỚC 2: KẾT NỐI APIFY ---
        current_step = "Apify Crawl"
        logger.info("Connecting to Apify...")
        
        run_input = {"hashtags": ["fyp"], "resultsPerPage": 10}
        
        # Nếu Apify sai token/mạng lỗi, hàm này văng lỗi -> Nhảy xuống except -> Gửi mail
        run_id, dataset_id, items = apify_service.run_actor(
            config.APIFY_ACTOR, 
            config.APIFY_TOKEN, 
            run_input
        )
        logger.info("Apify run success: %s (%d items)", run_id, len(items))

        # --- BƯỚC 3: LƯU FILE ---
        current_step = "Save File"
        ts = datetime.now().strftime("%d%m%YT%H%M%SZ")
        fname = f"{config.DEVICE_ID}_run_{ts}.json"
        fpath = os.path.join(config.STORAGE_PATH, fname)
        
        with open(fpath, "w", encoding="utf-8") as fw:
            json.dump(items, fw, ensure_ascii=False)

        # --- BƯỚC 4: THÔNG BÁO THÀNH CÔNG ---
        current_step = "Success Report"
        
        # Ghi log DB (Nếu lỗi ở đây thì chỉ in log, không gửi mail báo lỗi vì job đã xong rồi)
        db.log_to_db("SUCCESS", fname, total_record=len(items), id_config=id_config)
        
        notification.send_notification(
            "SUCCESS", 
            f"Crawl thành công {len(items)} items.", 
            f"File: {fname}\nRun ID: {run_id}"
        )
        logger.info("Job finished successfully.")

    except Exception as e:
        logger.exception("Job Failed at step: %s", current_step)
        
        # ===> ƯU TIÊN 1: GỬI MAIL BÁO LỖI NGAY LẬP TỨC <===
        error_msg = f"Lỗi nghiêm trọng tại bước: {current_step}"
        detail = f"Lỗi: {str(e)}\nConfig ID: {id_config}"
        
        try:
            notification.send_notification("FAILED", error_msg, detail)
        except Exception as mail_err:
            logger.error("Không thể gửi mail báo lỗi: %s", mail_err)

        # ===> ƯU TIÊN 2: CỐ GHI LOG LỖI VÀO DB (Nếu DB còn sống) <===
        try:
            db.log_to_db("FAILED", current_step, total_record=0, error_message=str(e), id_config=id_config)
        except:
            logger.error("Cũng không thể ghi log lỗi vào DB (có thể do DB sập).")