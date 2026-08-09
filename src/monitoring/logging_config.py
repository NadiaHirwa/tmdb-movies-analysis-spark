"""
logging_config.py

Central logging configuration for the entire pipeline. Every module
imports get_logger() from here instead of configuring logging itself,
so log format and behavior stay consistent across the whole project.
"""

import logging
import sys
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent.parent / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger for the given module name.

    Logs to both the console and a rotating log file, using UTF-8
    encoding explicitly so non-ASCII text (e.g. movie titles or
    language names in other scripts) never crashes on Windows'
    default console encoding.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger