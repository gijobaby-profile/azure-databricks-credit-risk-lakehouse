# =====================================================================
# Type        : Python File
# File        : src/logging/error_logger.py
# Purpose     : Error logging for Bronze ingestion
# =====================================================================

import traceback
import uuid

# =====================================================================
# to escape the ' used as appostope eg 'beauro's_file' and handle Null value
# =====================================================================
def _escape(value) -> str:
    return "" if value is None else str(value).replace("'", "''")

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

    spark.sql(f"""
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
            '{_escape(pipeline_run_id)}',
            'bronze_copy_into',
            '{_escape(entity_name)}',
            '{_escape(table_name)}',
            'TECHNICAL',
            'BRONZE_COPY_INTO_FAILED',
            '{_escape(str(error))}',
            '{_escape(traceback.format_exc())}',
            '{_escape(source_file_path)}',
            CAST(NULL AS STRING),
            'HIGH',
            current_timestamp(),
            current_date()
    """)