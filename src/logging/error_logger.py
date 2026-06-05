# =====================================================================
# Type        : Python File
# File        : src/logging/error_logger.py
# Purpose     : Error logging for Bronze ingestion
# =====================================================================

import random
import time
import traceback
import uuid
from src.utils.sql_utils import escape_sql, bool_to_sql

# =====================================================================
# Detect retryable Delta concurrency conflicts using string matching.
# =====================================================================
def _is_retryable_delta_error(error: Exception) -> bool:

    error_text = str(error)

    retryable_markers = [
        "ConcurrentAppendException",
        "ConcurrentModificationException",
        "MetadataChangedException",
        "ProtocolChangedException",
        "DELTA_CONCURRENT_APPEND",
        "DELTA_CONCURRENT_MODIFICATION",
        "DELTA_METADATA_CHANGED",
        "DELTA_PROTOCOL_CHANGED",
        "Files were added to partition",
        "conflicting commit",
        "Please try the operation again",
    ]

    return any(marker in error_text for marker in retryable_markers)


# =====================================================================
#  Execute only the supplied error-log SQL with retry.
#  Important:
#    - This retries only the error_log INSERT statement.
#    - It does not rerun the failed pipeline/notebook.
# =====================================================================
def _execute_sql_with_retry(
    spark,
    sql_text: str,
    operation_name: str,
    max_retries: int = 5,
    base_sleep_seconds: float = 2.0,
    max_sleep_seconds: float = 30.0,
) -> None:

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            spark.sql(sql_text)
            return

        except Exception as error:
            if not _is_retryable_delta_error(error):
                raise

            last_error = error

            if attempt >= max_retries:
                break

            sleep_seconds = min(
                max_sleep_seconds,
                base_sleep_seconds * (2 ** (attempt - 1)) + random.uniform(0, 1.5)
            )

            try:
                print(
                    f"WARNING [{operation_name}] Delta concurrency conflict. "
                    f"Retrying error-log SQL attempt {attempt + 1}/{max_retries} "
                    f"after {sleep_seconds:.2f} seconds. Error: {str(error)[:500]}"
                )
            except Exception:
                pass

            time.sleep(sleep_seconds)

    raise last_error

# =====================================================================
# Error log into audit.error_log
# =====================================================================
def log_error(
    spark,
    catalog_name: str,
    pipeline_run_id: str,
    config: dict,
    error: Exception,
    failed_record_json: str = None
) -> None:

    table_name = config.get("target_table_full_name", "UNKNOWN") if config else "UNKNOWN"
    source_file_path = config.get("source_path", "UNKNOWN") if config else "UNKNOWN"
    entity_name = config.get("entity_name", "UNKNOWN") if config else "UNKNOWN"

    failed_record_sql = (
        "CAST(NULL AS STRING)"
        if failed_record_json is None
        else f"'{escape_sql(failed_record_json)}'"
    )

    sql_text = f"""
        INSERT INTO {catalog_name}.audit.error_log
        (
            error_id,
            pipeline_run_id,
            job_name,
            task_name,
            table_name,
            error_type,
            error_code,
            error_message,
            error_stack_trace,
            source_file_path,
            failed_record_json,
            severity,
            created_timestamp,
            created_date
        )
        SELECT
            '{str(uuid.uuid4())}',
            '{escape_sql(pipeline_run_id)}',
            'bronze_copy_into',
            '{escape_sql(entity_name)}',
            '{escape_sql(table_name)}',
            'TECHNICAL',
            'BRONZE_COPY_INTO_FAILED',
            '{escape_sql(str(error))}',
            '{escape_sql(traceback.format_exc())}',
            '{escape_sql(source_file_path)}',
            {failed_record_sql},
            'HIGH',
            current_timestamp(),
            current_date()
    """

    _execute_sql_with_retry(
        spark=spark,
        sql_text=sql_text,
        operation_name="log_error"
    )