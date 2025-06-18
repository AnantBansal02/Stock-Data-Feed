# app/logging_config.py
import logging
import os
from logging.handlers import TimedRotatingFileHandler

def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "app.log")
    handler = TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=14, encoding='utf-8'
    )
    handler.suffix = "%Y-%m-%d"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)

    # Root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    # Attach handler to uvicorn loggers too
    uvicorn_loggers = ["uvicorn", "uvicorn.access", "uvicorn.error"]
    for uvicorn_logger in uvicorn_loggers:
        l = logging.getLogger(uvicorn_logger)
        l.setLevel(logging.INFO)
        l.addHandler(handler)
