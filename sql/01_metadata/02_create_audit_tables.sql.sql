-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create audit and operational logging tables

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.audit.pipeline_run_log (
    pipeline_run_id        STRING      COMMENT 'Unique pipeline run identifier generated once per workflow execution',
    pipeline_name          STRING      COMMENT 'Pipeline or workflow name',
    job_id                 STRING      COMMENT 'Databricks job ID',
    job_run_id             STRING      COMMENT 'Databricks job run ID',
    environment            STRING      COMMENT 'Execution environment such as dev, test, or prod',
    trigger_type           STRING      COMMENT 'manual, scheduled, api, retry',
    start_timestamp        TIMESTAMP   COMMENT 'Pipeline start timestamp',
    end_timestamp          TIMESTAMP   COMMENT 'Pipeline end timestamp',
    status                 STRING      COMMENT 'STARTED, SUCCESS, FAILED, PARTIAL_SUCCESS',
    records_read           BIGINT      COMMENT 'Total records read across the pipeline',
    records_written        BIGINT      COMMENT 'Total records written across the pipeline',
    records_rejected       BIGINT      COMMENT 'Total rejected records across the pipeline',
    error_message          STRING      COMMENT 'Pipeline-level error message when failed',
    created_by             STRING      COMMENT 'User or service principal that started the run',
    created_timestamp      TIMESTAMP   COMMENT 'Audit record creation timestamp',
    created_date           DATE        COMMENT 'Audit record creation date for partition pruning'
)
USING DELTA
PARTITIONED BY (created_date)
COMMENT 'Pipeline-level audit log for Credit Risk Lakehouse workflows'
;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.audit.table_load_log (
    table_load_id          STRING      COMMENT 'Unique table load identifier',
    pipeline_run_id        STRING      COMMENT 'Parent pipeline run identifier',
    job_name               STRING      COMMENT 'Databricks job name',
    task_name              STRING      COMMENT 'Databricks task or notebook name',
    source_system_id       STRING      COMMENT 'Source system identifier from config',
    source_file_name       STRING      COMMENT 'Source file name processed by the load',
    source_file_path       STRING      COMMENT 'Full source file path processed by the load',
    target_table_name      STRING      COMMENT 'Fully qualified target table name',
    load_type              STRING      COMMENT 'FULL, INCREMENTAL, or APPEND',
    write_mode             STRING      COMMENT 'append, overwrite, or merge',
    start_timestamp        TIMESTAMP   COMMENT 'Table load start timestamp',
    end_timestamp          TIMESTAMP   COMMENT 'Table load end timestamp',
    status                 STRING      COMMENT 'STARTED, SUCCESS, FAILED, SKIPPED',
    records_read           BIGINT      COMMENT 'Number of records read',
    records_written        BIGINT      COMMENT 'Number of records written',
    records_rejected       BIGINT      COMMENT 'Number of records rejected',
    source_file_count      INT         COMMENT 'Number of source files processed',
    error_message          STRING      COMMENT 'Table-level error message when failed',
    created_timestamp      TIMESTAMP   COMMENT 'Audit record creation timestamp',
    created_date           DATE        COMMENT 'Audit record creation date for partition pruning'
)
USING DELTA
PARTITIONED BY (created_date)
COMMENT 'Table-level load audit log for ingestion and transformation jobs'
;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.audit.error_log (
    error_id               STRING      COMMENT 'Unique error identifier',
    pipeline_run_id        STRING      COMMENT 'Pipeline run identifier',
    job_name               STRING      COMMENT 'Databricks job name',
    task_name              STRING      COMMENT 'Databricks task or notebook name',
    table_name             STRING      COMMENT 'Table name involved in the error',
    error_type             STRING      COMMENT 'technical, data_quality, validation, permission, schema',
    error_code             STRING      COMMENT 'System or application error code',
    error_message          STRING      COMMENT 'Short error message',
    error_stack_trace      STRING      COMMENT 'Detailed exception stack trace',
    source_file_path       STRING      COMMENT 'Source file path involved in the error, if applicable',
    failed_record_json     STRING      COMMENT 'Failed record serialized as JSON, if applicable',
    severity               STRING      COMMENT 'LOW, MEDIUM, HIGH, CRITICAL',
    created_timestamp      TIMESTAMP   COMMENT 'Error record creation timestamp',
    created_date           DATE        COMMENT 'Error record creation date for partition pruning'
)
USING DELTA
PARTITIONED BY (created_date)
COMMENT 'Centralized error log for Credit Risk Lakehouse pipeline failures'
;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.audit.operational_metrics (
    metric_id              STRING      COMMENT 'Unique metric identifier',
    pipeline_run_id        STRING      COMMENT 'Pipeline run identifier',
    metric_date            DATE        COMMENT 'Metric business or processing date',
    table_name             STRING      COMMENT 'Table name related to the metric',
    metric_name            STRING      COMMENT 'Metric name such as duration_seconds or rows_per_second',
    metric_value           DOUBLE      COMMENT 'Metric numeric value',
    metric_unit            STRING      COMMENT 'Metric unit such as rows, seconds, percent, or MB',
    threshold_value        DOUBLE      COMMENT 'Expected threshold value where applicable',
    threshold_status       STRING      COMMENT 'WITHIN_THRESHOLD, BREACHED, NOT_APPLICABLE',
    created_timestamp      TIMESTAMP   COMMENT 'Metric record creation timestamp'
)
USING DELTA
PARTITIONED BY (metric_date)
COMMENT 'Operational metrics table for monitoring pipeline performance and data volumes'
;

