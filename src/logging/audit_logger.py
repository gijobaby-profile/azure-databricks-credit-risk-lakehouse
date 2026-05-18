# =====================================================================
# Type        : Python File
# File        : src/logging/audit_logger.py
# Purpose     : Audit logging for Bronze ingestion
# =====================================================================

import uuid
from src.utils.sql_utils import escape_sql, bool_to_sql

# =====================================================================
# Creates one STARTED record in pipeline_run_log when the Bronze pipeline starts.
# =====================================================================
def start_pipeline(spark, catalog_name: str, pipeline_run_id: str, entity_name: str) -> None:
    spark.sql(f"""
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
        WHERE pipeline_run_id = '{escape_sql(pipeline_run_id)}'
    """)

# =====================================================================
# Updates pipeline_run_log as FAILED if the pipeline fails.
# =====================================================================
def end_pipeline_failure(spark, catalog_name: str, pipeline_run_id: str, error_message: str) -> None:
    spark.sql(f"""
        UPDATE {catalog_name}.audit.pipeline_run_log
        SET end_timestamp = current_timestamp(),
            status = 'FAILED',
            error_message = '{escape_sql(error_message)}'
        WHERE pipeline_run_id = '{escape_sql(pipeline_run_id)}'
    """)

# =====================================================================
# Creates one STARTED record in table_load_log for one entity/table load.
# =====================================================================
def start_load(spark, catalog_name: str, pipeline_run_id: str, config: dict) -> str:
    table_load_id = str(uuid.uuid4())

    spark.sql(f"""
        INSERT INTO {catalog_name}.audit.table_load_log
        (
            table_load_id,
            pipeline_run_id,
            job_name,
            task_name,
            source_system_id,
            source_file_name,
            source_file_path,
            target_table_name,
            load_type,
            write_mode,
            start_timestamp,
            end_timestamp,
            status,
            records_read,
            records_written,
            records_rejected,
            source_file_count,
            error_message,
            created_timestamp,
            created_date
        )
        SELECT
            '{table_load_id}',
            '{escape_sql(pipeline_run_id)}',
            'bronze_copy_into',
            '{escape_sql(config["entity_name"])}',
            '{escape_sql(config["source_system"])}',
            '{escape_sql(config["entity_name"])}',
            '{escape_sql(config["source_path"])}',
            '{escape_sql(config["target_table_full_name"])}',
            'APPEND',
            'COPY_INTO',
            current_timestamp(),
            CAST(NULL AS TIMESTAMP),
            'STARTED',
            CAST(0 AS BIGINT),
            CAST(0 AS BIGINT),
            CAST(0 AS BIGINT),
            CAST(NULL AS INT),
            CAST(NULL AS STRING),
            current_timestamp(),
            current_date()
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
        WHERE table_load_id = '{escape_sql(table_load_id)}'
    """)

# =====================================================================
# Updates table_load_log as FAILED if one entity/table load fails.
# =====================================================================
def end_load_failure(spark, catalog_name: str, table_load_id: str, error_message: str) -> None:
    spark.sql(f"""
        UPDATE {catalog_name}.audit.table_load_log
        SET end_timestamp = current_timestamp(),
            status = 'FAILED',
            error_message = '{escape_sql(error_message)}'
        WHERE table_load_id = '{escape_sql(table_load_id)}'
    """)
