# =====================================================================
# Type        : Python file
# File        : src/utils/logger_utils.py
# Purpose     : Common Python logger utility
# =====================================================================

import logging
import sys
from pathlib import Path
from typing import Optional


# =====================================================================
# Create and return logger
# =====================================================================
def get_logger(
    name: str = "credit_risk_lakehouse",
    log_file_path: Optional[str] = None
) -> logging.Logger:
    """
    Create and return a Databricks-safe logger.

    Parameters
    ----------
    name:
        Logger name. Example: bronze_ingestion.

    log_file_path:
        Optional file path for file-based logging.
        If not provided, only console logging is used.

    Returns
    -------
    logging.Logger
        Configured logger.
    """

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Important:
    # Prevent duplicate messages through the root logger.
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    # -----------------------------------------------------------------
    # Console handler
    # -----------------------------------------------------------------
    # Remove existing plain StreamHandlers to avoid duplicate logs in
    # Databricks all-purpose clusters/notebook reruns.
    for handler in list(logger.handlers):
        if type(handler) is logging.StreamHandler:
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # -----------------------------------------------------------------
    # File handler
    # -----------------------------------------------------------------
    if log_file_path:
        log_path = Path(log_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler_exists = any(
            isinstance(handler, logging.FileHandler)
            and Path(handler.baseFilename) == log_path
            for handler in logger.handlers
        )

        if not file_handler_exists:
            file_handler = logging.FileHandler(
                str(log_path),
                mode="a",
                encoding="utf-8"
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger


# =====================================================================
# Log message to driver/job logs and notebook cell output
# =====================================================================
def log_step(logger: logging.Logger, message: str) -> None:
    """
    Log normal pipeline step.

    Important:
    Do not force-flush Databricks notebook stdout StreamHandler.
    Databricks notebook output streams can raise:
        OSError: [Errno 29] Illegal seek
    during flush().
    """

    try:
        logger.info(message)
    except Exception:
        # Logging must not break the ETL pipeline.
        pass

    # Keep print output for notebook visibility.
    # This may print the message once in addition to logger.info().
    # If you want only one output line, comment out this print block.
    try:
        print(message)
    except Exception:
        pass

    # Flush only file handlers.
    # Do NOT flush console StreamHandler in Databricks notebooks/jobs.
    for handler in logger.handlers:
        try:
            if isinstance(handler, logging.FileHandler):
                handler.flush()
        except Exception:
            pass


# =====================================================================
# Log warning message
# =====================================================================
def log_warning(logger: logging.Logger, message: str) -> None:
    """
    Log warning message safely.
    """

    try:
        logger.warning(message)
    except Exception:
        try:
            print(f"WARNING: {message}")
        except Exception:
            pass

    for handler in logger.handlers:
        try:
            if isinstance(handler, logging.FileHandler):
                handler.flush()
        except Exception:
            pass


# =====================================================================
# Log error message
# =====================================================================
def log_error(
    logger: logging.Logger,
    message: str,
    exception: Optional[Exception] = None
) -> None:
    """
    Log error message safely.

    The caller should still raise the exception after writing audit records
    so that Databricks Jobs and ADF correctly receive FAILED status.
    """

    try:
        if exception is not None:
            logger.exception(f"{message} | Error: {str(exception)}")
        else:
            logger.error(message)
    except Exception:
        try:
            print(f"ERROR: {message}")
            if exception is not None:
                print(f"Exception: {str(exception)}")
        except Exception:
            pass

    for handler in logger.handlers:
        try:
            if isinstance(handler, logging.FileHandler):
                handler.flush()
        except Exception:
            pass


# =====================================================================
# Close logger
# =====================================================================
def close_logger(logger: logging.Logger) -> None:
    """
    Close logger handlers safely.

    Flush only FileHandler objects. Do not flush Databricks notebook stdout.
    """

    for handler in logger.handlers[:]:
        try:
            if isinstance(handler, logging.FileHandler):
                handler.flush()
        except Exception:
            pass

        try:
            handler.close()
        except Exception:
            pass

        try:
            logger.removeHandler(handler)
        except Exception:
            pass
