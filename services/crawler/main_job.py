# main_job.py
import os
import json
from datetime import datetime
import config
from logging_setup import logger
import db
import apify_service
import notification # Import module đã sửa

def job():
    """Pipeline công việc chính."""
    id_config = None 
    current_step = "Init"

    try:
        current_step = "Load Config & DB"
        logger.info("Job started for device %s", config.DEVICE_ID)

        # ... (Code Load Config giữ nguyên) ...
        id_config = db.save_config_to_db() or 1

        # ... (Code Run Apify giữ nguyên) ...
        current_step = "Apify Crawl"
        run_input = {"hashtags": ["fyp"], "resultsPerPage": 3}
        run_id, dataset_id, items = apify_service.run_actor(config.APIFY_ACTOR, config.APIFY_TOKEN, run_input)

        # ... (Code Save File giữ nguyên) ...
        current_step = "Save File"
        ts = datetime.now().strftime("%d%m%YT%H%M%SZ")
        fname = f"{config.DEVICE_ID}_run_{ts}.json"
        fpath = os.path.join(config.STORAGE_PATH, fname)
        with open(fpath, "w", encoding="utf-8") as fw:
            json.dump(items, fw, ensure_ascii=False)

        # 5️⃣ Ghi log DB thành công
        current_step = "Log DB Success"
        db.log_to_db("SUCCESS", fname, total_record=len(items), id_config=id_config)
        
        # ===> GỬI EMAIL THÀNH CÔNG <===
        success_msg = f"Đã thu thập thành công {len(items)} dòng dữ liệu."
        success_detail = f"File saved at: {fpath}\nRun ID: {run_id}"
        notification.send_notification("SUCCESS", success_msg, success_detail)

        logger.info("Job finished successfully.")

    except Exception as e:
        logger.exception("Job failed: %s", e)
        
        # Ghi log DB thất bại
        db.log_to_db("FAILED", current_step, total_record=0, error_message=str(e), id_config=id_config)
        
        # ===> GỬI EMAIL THẤT BẠI <===
        error_msg = f"Lỗi xảy ra tại bước: {current_step}"
        notification.send_notification("FAILED", error_msg, detail_info=str(e))