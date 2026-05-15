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
def log_error(spark, catalog_name: str, pipeline_run_id: str, config: dict, error: Exception) -> None:
    table_name = config.get("target_table_full_name", "UNKNOWN") if config else "UNKNOWN"
    source_path = config.get("source_path", "UNKNOWN") if config else "UNKNOWN"
    entity_name = config.get("entity_name", "UNKNOWN") if config else "UNKNOWN"

    spark.sql(f"""
        INSERT INTO {catalog_name}.audit.error_log
        SELECT
            '{str(uuid.uuid4())}' AS error_id,
            '{_escape(pipeline_run_id)}' AS pipeline_run_id,
            'bronze_copy_into' AS job_name,
            '{_escape(entity_name)}' AS task_name,
            '{_escape(table_name)}' AS table_name,
            'TECHNICAL' AS error_type,
            'BRONZE_COPY_INTO_FAILED' AS error_code,
            '{_escape(str(error))}' AS error_message,
            '{_escape(traceback.format_exc())}' AS error_stack_trace,
            '{_escape(source_path)}' AS source_path,
            'HIGH' AS severity,
            current_timestamp() AS created_timestamp
    """)
