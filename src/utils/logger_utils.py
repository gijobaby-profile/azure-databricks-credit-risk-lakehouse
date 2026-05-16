# =====================================================================
# Type        : Python file
# File        : src/utils/logger_utils.py
# Purpose     : Common Python logger utility
# =====================================================================

import logging
import sys
from pathlib import Path


# =====================================================================
# Create and return logger
# =====================================================================
def get_logger(
    name: str = "credit_risk_lakehouse",
    log_file_path: str = None
) -> logging.Logger:

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    # Console handler
    if not any(type(handler) is logging.StreamHandler for handler in logger.handlers):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # File handler
    if log_file_path:
        log_path = Path(log_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        if not any(
            isinstance(handler, logging.FileHandler)
            and Path(handler.baseFilename) == log_path
            for handler in logger.handlers
        ):
            file_handler = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger


# =====================================================================
# Log message to driver/job logs and notebook cell output
# =====================================================================
def log_step(logger: logging.Logger, message: str) -> None:
    logger.info(message)
    print(message)

    # Force immediate write to file/driver logs
    for handler in logger.handlers:
        handler.flush()
        

# =====================================================================
# close logger
# =====================================================================
       
def close_logger(logger: logging.Logger) -> None:
    for handler in logger.handlers[:]:
        handler.flush()
        handler.close()
        logger.removeHandler(handler)
