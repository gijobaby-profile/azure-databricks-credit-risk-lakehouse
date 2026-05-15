# =====================================================================
# Type        : Python File
# File        : src/logging/audit_logger.py
# Purpose     : Audit logging for Bronze ingestion
# =====================================================================

import uuid

# =====================================================================
# to escape the ' used as appostope eg 'beauro's_file' and handle Null value
# =====================================================================

def _escape(value) -> str:
    return "" if value is None else str(value).replace("'", "''")

# =====================================================================
# Creates one STARTED record in pipeline_run_log when the Bronze pipeline starts.
# =====================================================================

def start_pipeline(spark, catalog_name: str, pipeline_run_id: str, entity_name: str) -> None:
    spark.sql(f"""
        INSERT INTO {catalog_name}.audit.pipeline_run_log
        SELECT
            '{_escape(pipeline_run_id)}' AS pipeline_run_id,
            'bronze_copy_into' AS pipeline_name,
            CAST(NULL AS STRING) AS job_id,
            CAST(NULL AS STRING) AS run_id,
            'dev' AS environment,
            'manual' AS trigger_type,
            current_timestamp() AS start_timestamp,
            CAST(NULL AS TIMESTAMP) AS end_timestamp,
            'STARTED' AS status,
            CAST(0 AS BIGINT) AS records_read,
            CAST(0 AS BIGINT) AS records_written,
            CAST(0 AS BIGINT) AS records_rejected,
            CAST(NULL AS STRING) AS error_message,
            current_user() AS created_by,
            current_timestamp() AS created_timestamp
    """)

# =====================================================================
# Updates pipeline_run_log as SUCCESS after the Bronze load completes.
# =====================================================================

def end_pipeline_success(spark, catalog_name: str, pipeline_run_id: str, records_written: int) -> None:
    spark.sql(f"""
        UPDATE {catalog_name}.audit.pipeline_run_log
        SET end_timestamp = current_timestamp(),
            status = 'SUCCESS',
            records_read = {records_written},
            records_written = {records_written},
            records_rejected = 0
        WHERE pipeline_run_id = '{_escape(pipeline_run_id)}'
    """)

# =====================================================================
# Updates pipeline_run_log as FAILED if the pipeline fails.
# =====================================================================
def end_pipeline_failure(spark, catalog_name: str, pipeline_run_id: str, error_message: str) -> None:
    spark.sql(f"""
        UPDATE {catalog_name}.audit.pipeline_run_log
        SET end_timestamp = current_timestamp(),
            status = 'FAILED',
            error_message = '{_escape(error_message)}'
        WHERE pipeline_run_id = '{_escape(pipeline_run_id)}'
    """)

# =====================================================================
# Creates one STARTED record in table_load_log for one entity/table load.
# =====================================================================
def start_load(spark, catalog_name: str, pipeline_run_id: str, config: dict) -> str:
    table_load_id = str(uuid.uuid4())

    spark.sql(f"""
        INSERT INTO {catalog_name}.audit.table_load_log
        SELECT
            '{table_load_id}' AS table_load_id,
            '{_escape(pipeline_run_id)}' AS pipeline_run_id,
            'bronze_copy_into' AS job_name,
            '{_escape(config["entity_name"])}' AS task_name,
            '{_escape(config["source_system"])}' AS source_system_id,
            '{_escape(config["entity_name"])}' AS source_file_name,
            '{_escape(config["source_path"])}' AS source_path,
            '{_escape(config["target_table_full_name"])}' AS target_table_name,
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
            current_timestamp() AS created_timestamp
    """)
    return table_load_id

# =====================================================================
# Updates table_load_log as SUCCESS after one table/entity load completes
# =====================================================================
def end_load_success(spark, catalog_name: str, table_load_id: str, records_written: int) -> None:
    spark.sql(f"""
        UPDATE {catalog_name}.audit.table_load_log
        SET end_timestamp = current_timestamp(),
            status = 'SUCCESS',
            records_read = {records_written},
            records_written = {records_written},
            records_rejected = 0
        WHERE table_load_id = '{_escape(table_load_id)}'
    """)

# =====================================================================
# Updates table_load_log as FAILED if one entity/table load fails.
# =====================================================================
def end_load_failure(spark, catalog_name: str, table_load_id: str, error_message: str) -> None:
    spark.sql(f"""
        UPDATE {catalog_name}.audit.table_load_log
        SET end_timestamp = current_timestamp(),
            status = 'FAILED',
            error_message = '{_escape(error_message)}'
        WHERE table_load_id = '{_escape(table_load_id)}'
    """)
