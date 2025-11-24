# app.py
from apscheduler.schedulers.blocking import BlockingScheduler
import config
from logging_setup import logger
from main_job import job  # Import a-giai-đoạn-job chính

def start_scheduler():
    """Initializes and starts the job scheduler."""
    sched = BlockingScheduler(timezone=config.SCHEDULE_TIMEZONE)
    cron_expr = config.SCHEDULE_CRON

    if config.SCHEDULE_ENABLED and cron_expr:
        try:
            parts = cron_expr.split()
            if len(parts) == 5:
                minute, hour, day, month, day_of_week = parts
                sched.add_job(job, 'cron', minute=minute, hour=hour, day=day,
                              month=month, day_of_week=day_of_week, id="apify_job")
                logger.info("Scheduled job with cron: %s", cron_expr)
            else:
                logger.warning("Invalid cron expression '%s', running once immediately.", cron_expr)
                job() # Chạy 1 lần nếu cron sai
        except ValueError:
             logger.error("Invalid cron format: %s. Running once.", cron_expr)
             job() # Chạy 1 lần nếu cron sai
    else:
        logger.info("Scheduler disabled or no cron expression; running once.")
        job() # Chạy 1 lần nếu không bật schedule
        return # Kết thúc nếu chỉ chạy 1 lần

    # Chỉ chạy sched.start() nếu có schedule
    try:
        logger.info("Scheduler started. Press Ctrl+C to exit.")
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")

if __name__ == "__main__":
    start_scheduler()