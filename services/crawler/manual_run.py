# manual_run.py
from main_job import job
from logging_setup import logger

if __name__ == "__main__":
    logger.info(">>> MANUAL TRIGGER STARTED <<<")
    job()
    logger.info(">>> MANUAL TRIGGER FINISHED <<<")