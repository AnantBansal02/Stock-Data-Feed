# app/jobs/scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.base import JobLookupError
from apscheduler.executors.pool import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(
    executors={'default': ThreadPoolExecutor(10)},
    job_defaults={'coalesce': False, 'max_instances': 1},
    timezone='UTC',
)

def start_scheduler():
    try:
        scheduler.start()
        logger.info("Scheduler started.")
    except Exception as e:
        logger.error(f"Error starting scheduler: {e}")

def shutdown_scheduler():
    try:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shut down.")
    except JobLookupError:
        logger.warning("Scheduler already shut down.")
