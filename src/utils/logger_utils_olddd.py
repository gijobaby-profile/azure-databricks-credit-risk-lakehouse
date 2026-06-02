# Databricks notebook source
"""
logger_utils.py

Databricks-safe logging utility.

Purpose:
- Provides reusable logger setup for Bronze, Silver, Conformance, and Gold notebooks.
- Avoids duplicate log messages during repeated notebook/job runs.
- Avoids Databricks notebook stream flush errors such as:
  OSError: [Errno 29] Illegal seek
- Keeps logging failures from breaking the ETL pipeline.

Recommended usage:

    from src.utils.logger_utils import get_logger, log_step, log_error

    logger = get_logger("bronze_ingestion")
    log_step(logger, "Starting Bronze ingestion")

    try:
        ...
    except Exception as e:
        log_error(logger, "Bronze ingestion failed", e)
        raise
"""

import logging
import sys
from typing import Optional


DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class DatabricksSafeStreamHandler(logging.StreamHandler):
    """
    StreamHandler variant that is safer for Databricks notebooks/jobs.

    In Databricks notebook environments, the underlying output stream can
    sometimes raise:

        OSError: [Errno 29] Illegal seek

    during flush(). This should not break the ETL pipeline.
    """

    def flush(self) -> None:
        try:
            if self.stream and hasattr(self.stream, "flush"):
                self.stream.flush()
        except OSError:
            # Do not fail the notebook/job because console stream flushing failed.
            pass
        except Exception:
            # Logging must not break pipeline execution.
            pass


def _resolve_log_level(log_level: str | int) -> int:
    """
    Convert log level from string/int to logging level.

    Examples:
        "INFO"  -> logging.INFO
        "DEBUG" -> logging.DEBUG
        20      -> logging.INFO
    """

    if isinstance(log_level, int):
        return log_level

    if not isinstance(log_level, str):
        return logging.INFO

    normalized_level = log_level.strip().upper()

    return {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }.get(normalized_level, logging.INFO)


def get_logger(
    logger_name: str,
    log_level: str | int = logging.INFO,
    reset_handlers: bool = True,
) -> logging.Logger:
    """
    Create and return a Databricks-safe logger.

    Parameters
    ----------
    logger_name:
        Name of the logger. Example: "bronze_ingestion".

    log_level:
        Logging level. Accepts string or logging integer.
        Example: "INFO", "DEBUG", logging.INFO.

    reset_handlers:
        If True, removes existing handlers before adding a new handler.
        This is important in Databricks notebooks because rerunning cells can
        attach duplicate handlers and print duplicate log messages.

    Returns
    -------
    logging.Logger
        Configured logger object.
    """

    resolved_level = _resolve_log_level(log_level)

    logger = logging.getLogger(logger_name)
    logger.setLevel(resolved_level)

    if reset_handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

    handler = DatabricksSafeStreamHandler(sys.stdout)
    handler.setLevel(resolved_level)

    formatter = logging.Formatter(
        fmt=DEFAULT_LOG_FORMAT,
        datefmt=DEFAULT_DATE_FORMAT,
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    # Prevent duplicate logs from being propagated to the root logger.
    logger.propagate = False

    return logger


def log_step(logger: logging.Logger, message: str) -> None:
    """
    Log a normal pipeline step.

    Logging should never fail the pipeline. If logger.info() fails because of
    Databricks notebook output stream issues, fallback to print().
    """

    try:
        logger.info(message)
    except Exception:
        try:
            print(message)
        except Exception:
            pass


def log_warning(logger: logging.Logger, message: str) -> None:
    """
    Log a warning message safely.
    """

    try:
        logger.warning(message)
    except Exception:
        try:
            print(f"WARNING: {message}")
        except Exception:
            pass


def log_error(
    logger: logging.Logger,
    message: str,
    exception: Optional[Exception] = None,
) -> None:
    """
    Log an error message safely.

    Parameters
    ----------
    logger:
        Logger object created by get_logger().

    message:
        Error message.

    exception:
        Optional exception object. If provided, stack trace is logged using
        logger.exception().
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


def log_debug(logger: logging.Logger, message: str) -> None:
    """
    Log debug message safely.
    """

    try:
        logger.debug(message)
    except Exception:
        try:
            print(f"DEBUG: {message}")
        except Exception:
            pass


def close_logger(logger: logging.Logger) -> None:
    """
    Close and remove all handlers from a logger.

    Useful in Databricks notebooks if you want to explicitly clean logger state.
    """

    try:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
    except Exception:
        pass