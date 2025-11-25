# utils/logger.py
import logging
import os
from datetime import datetime

def setup_logging(country=None):
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    if country:
        # Per-country log
        logger = logging.getLogger(country.upper())
        if not logger.handlers:
            handler = logging.FileHandler(os.path.join(log_dir, f"{country.upper()}.log"))
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    else:
        # MAIN LOG with file + console
        logger = logging.getLogger('main')
        if not logger.handlers:
            file_handler = logging.FileHandler(os.path.join(log_dir, "main.log"))
            file_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)

            logger.setLevel(logging.INFO)
        return logger
