# =====================================================================
# Type        : Python File
# File        : src/logging/audit_logger.py
# Purpose     : Audit logging for Bronze ingestion
# =====================================================================

import random
import time
import uuid
from typing import Optional
from src.utils.sql_utils import escape_sql, bool_to_sql, safe_int

# =====================================================================
# Detect retryable Delta concurrency conflicts using string matching.
# Databricks can expose different concrete exception classes depending on
# DBR version, Spark Connect/classic mode, and notebook/job execution.
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
# Execute only the supplied audit SQL with retry.
#    Important:
#    - This does NOT rerun the notebook.
#    - This does NOT reload Bronze/Silver/Gold data.
#    - This retries only the audit INSERT/UPDATE statement.
# =====================================================================
def _execute_sql_with_retry(
    spark,
    sql_text: str,
    operation_name: str,
    max_retries: int = 5,
    base_sleep_seconds: float = 2.0,
    max_sleep_seconds: float = 30.0,
) -> None:
    
    print(f"\n===== SQL DEBUG | {operation_name} =====")
    print(sql_text)
    print("===== END SQL DEBUG =====\n")

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
                    f"Retrying audit SQL attempt {attempt + 1}/{max_retries} "
                    f"after {sleep_seconds:.2f} seconds. Error: {str(error)[:500]}"
                )
            except Exception:
                pass

            time.sleep(sleep_seconds)

    raise last_error


# =====================================================================
# Creates one STARTED record in pipeline_run_log when the Bronze pipeline starts.
# =====================================================================
def start_pipeline(spark, catalog_name: str, pipeline_run_id: str, entity_name: str) -> None:
    sql_text = f"""
        INSERT INTO {catalog_name}.audit.pipeline_run_log
        (
            pipeline_run_id,
            pipeline_name,
            job_id,
            job_run_id,
            environment,
            trigger_type,
            start_timestamp,
            end_timestamp,
            status,
            records_read,
            records_written,
            records_rejected,
            error_message,
            created_by,
            created_timestamp,
            created_date
        )
        SELECT
            '{escape_sql(pipeline_run_id)}',
            'bronze_copy_into',
            CAST(NULL AS STRING),
            CAST(NULL AS STRING),
            'dev',
            'manual',
            current_timestamp(),
            CAST(NULL AS TIMESTAMP),
            'STARTED',
            CAST(0 AS BIGINT),
            CAST(0 AS BIGINT),
            CAST(0 AS BIGINT),
            CAST(NULL AS STRING),
            current_user(),
            current_timestamp(),
            current_date()
    """

    _execute_sql_with_retry(
        spark=spark,
        sql_text=sql_text,
        operation_name="start_pipeline"
    )



# =====================================================================
# Updates pipeline_run_log as SUCCESS after the Bronze load completes.
# =====================================================================

def end_pipeline_success(spark, catalog_name: str, pipeline_run_id: str, records_written: int) -> None:
    records_written = safe_int(records_written)

    sql_text = f"""
        UPDATE {catalog_name}.audit.pipeline_run_log
        SET end_timestamp = current_timestamp(),
            status = 'SUCCESS',
            records_read = {records_written},
            records_written = {records_written},
            records_rejected = 0
        WHERE pipeline_run_id = '{escape_sql(pipeline_run_id)}'
    """

    _execute_sql_with_retry(
        spark=spark,
        sql_text=sql_text,
        operation_name="end_pipeline_success"
    )

# =====================================================================
# Updates pipeline_run_log as FAILED if the pipeline fails.
# =====================================================================
def end_pipeline_failure(spark, catalog_name: str, pipeline_run_id: str, error_message: str) -> None:
    sql_text = f"""
        UPDATE {catalog_name}.audit.pipeline_run_log
        SET end_timestamp = current_timestamp(),
            status = 'FAILED',
            error_message = '{escape_sql(error_message)}'
        WHERE pipeline_run_id = '{escape_sql(pipeline_run_id)}'
    """

    _execute_sql_with_retry(
        spark=spark,
        sql_text=sql_text,
        operation_name="end_pipeline_failure"
    )

# =====================================================================
# Creates one STARTED record in table_load_log for one entity/table load.
# =====================================================================
def start_load(spark, catalog_name: str, pipeline_run_id: str, config: dict) -> str:
    table_load_id = str(uuid.uuid4())

    entity_name = config.get("entity_name", "UNKNOWN") if config else "UNKNOWN"
    source_system = config.get("source_system", "UNKNOWN") if config else "UNKNOWN"
    target_table = config.get("target_table_full_name", "UNKNOWN") if config else "UNKNOWN"

    source_file_name = (
        config.get("source_file_name")
        or config.get("entity_name")
        or "UNKNOWN"
    ) if config else "UNKNOWN"

    source_file_path = (
        config.get("source_file_path")
        or config.get("source_path")
        or config.get("source_table_full_name")
        or config.get("source_table")
        or "NOT_APPLICABLE"
    ) if config else "NOT_APPLICABLE"

    sql_text = f"""
        INSERT INTO {catalog_name}.audit.table_load_log BY NAME
        SELECT
            '{escape_sql(table_load_id)}' AS table_load_id,
            '{escape_sql(pipeline_run_id)}' AS pipeline_run_id,
            'bronze_copy_into' AS job_name,
            '{escape_sql(entity_name)}' AS task_name,
            '{escape_sql(source_system)}' AS source_system_id,
            '{escape_sql(source_file_name)}' AS source_file_name,
            '{escape_sql(source_file_path)}' AS source_file_path,
            '{escape_sql(target_table)}' AS target_table_name,
            'APPEND' AS load_type,
            'COPY_INTO' AS write_mode,
            current_timestamp() AS start_timestamp,
            CAST(NULL AS TIMESTAMP) AS end_timestamp,
            'STARTED' AS status,
            CAST(0 AS BIGINT) AS records_read,
            CAST(0 AS BIGINT) AS records_written,
            CAST(0 AS BIGINT) AS records_rejected,
            CAST(NULL AS INT) AS source_file_count,
            CAST(NULL AS STRING) AS error_message,
            current_timestamp() AS created_timestamp,
            current_date() AS created_date
    """

    print(f"\n===== START_LOAD SQL DEBUG =====\n{sql_text}\n===== END SQL DEBUG =====")

    _execute_sql_with_retry(
        spark=spark,
        sql_text=sql_text,
        operation_name="start_load"
    )

    return table_load_id

# =====================================================================
# Updates table_load_log as SUCCESS after one table/entity load completes
# =====================================================================
def end_load_success(spark, catalog_name: str, table_load_id: str, records_written: int) -> None:
    records_written = safe_int(records_written)

    sql_text = f"""
        UPDATE {catalog_name}.audit.table_load_log
        SET end_timestamp = current_timestamp(),
            status = 'SUCCESS',
            records_read = {records_written},
            records_written = {records_written},
            records_rejected = 0
        WHERE table_load_id = '{escape_sql(table_load_id)}'
    """

    _execute_sql_with_retry(
        spark=spark,
        sql_text=sql_text,
        operation_name="end_load_success"
    )

# =====================================================================
# Updates table_load_log as FAILED if one entity/table load fails.
# =====================================================================
def end_load_failure(spark, catalog_name: str, table_load_id: str, error_message: str) -> None:
    sql_text = f"""
        UPDATE {catalog_name}.audit.table_load_log
        SET end_timestamp = current_timestamp(),
            status = 'FAILED',
            error_message = '{escape_sql(error_message)}'
        WHERE table_load_id = '{escape_sql(table_load_id)}'
    """

    _execute_sql_with_retry(
        spark=spark,
        sql_text=sql_text,
        operation_name="end_load_failure"
    )
