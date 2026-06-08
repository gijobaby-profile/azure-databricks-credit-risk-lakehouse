-- Databricks notebook source
-- =====================================================================
-- File        : 04_adf_runtime_sql_templates.sql
-- Purpose     : SQL templates to use later from ADF Lookup/Script activities
-- =====================================================================

-- COMMAND ----------

-- =====================================================================
-- 1. Generate expected file rows for a business date
-- =====================================================================

INSERT INTO credit_risk_dev.orchestration.file_arrival_status
(
    business_dt,
    pipeline_run_id,
    file_id,
    source_system,
    entity_name,
    expected_data_file_name,
    expected_success_file_name,
    expected_data_file_path,
    expected_success_file_path,
    actual_data_file_name,
    actual_success_file_name,
    actual_data_file_path,
    actual_success_file_path,
    sla_deadline_ts,
    first_success_detected_ts,
    first_data_detected_ts,
    last_checked_ts,
    success_file_status,
    data_file_status,
    arrival_status,
    mandatory_flag,
    poll_count,
    bronze_silver_trigger_status,
    databricks_job_run_id,
    error_message,
    created_timestamp,
    updated_timestamp
)
SELECT
    DATE '<business_dt>' AS business_dt,
    '<pipeline_run_id>' AS pipeline_run_id,
    file_id,
    source_system,
    entity_name,

    replace(data_file_name_pattern, '${business_dt_yyyyMMdd}', '<business_dt_token>') AS expected_data_file_name,
    replace(success_file_name_pattern, '${business_dt_yyyyMMdd}', '<business_dt_token>') AS expected_success_file_name,

    concat(source_base_path, replace(data_file_name_pattern, '${business_dt_yyyyMMdd}', '<business_dt_token>')) AS expected_data_file_path,
    concat(source_base_path, replace(success_file_name_pattern, '${business_dt_yyyyMMdd}', '<business_dt_token>')) AS expected_success_file_path,

    CAST(NULL AS STRING) AS actual_data_file_name,
    CAST(NULL AS STRING) AS actual_success_file_name,
    CAST(NULL AS STRING) AS actual_data_file_path,
    CAST(NULL AS STRING) AS actual_success_file_path,

    to_timestamp(concat('<business_dt>', ' ', expected_arrival_time), 'yyyy-MM-dd HH:mm')
        + make_interval(0,0,0,0,0,sla_grace_minutes,0) AS sla_deadline_ts,

    CAST(NULL AS TIMESTAMP) AS first_success_detected_ts,
    CAST(NULL AS TIMESTAMP) AS first_data_detected_ts,
    CAST(NULL AS TIMESTAMP) AS last_checked_ts,

    'EXPECTED' AS success_file_status,
    'EXPECTED' AS data_file_status,
    'WAITING_FOR_SUCCESS' AS arrival_status,

    mandatory_flag,
    0 AS poll_count,
    'NOT_TRIGGERED' AS bronze_silver_trigger_status,
    CAST(NULL AS STRING) AS databricks_job_run_id,
    CAST(NULL AS STRING) AS error_message,

    current_timestamp() AS created_timestamp,
    current_timestamp() AS updated_timestamp
FROM credit_risk_dev.orchestration.file_ingestion_metadata
WHERE active_flag = true;

-- COMMAND ----------

-- =====================================================================
-- 2. Mark file as waiting
-- =====================================================================

UPDATE credit_risk_dev.orchestration.file_arrival_status
SET success_file_status = 'WAITING',
    data_file_status = 'WAITING',
    arrival_status = 'WAITING_FOR_SUCCESS',
    poll_count = poll_count + 1,
    last_checked_ts = current_timestamp(),
    updated_timestamp = current_timestamp()
WHERE business_dt = DATE '<business_dt>'
  AND pipeline_run_id = '<pipeline_run_id>'
  AND entity_name = '<entity_name>';

-- COMMAND ----------

-- =====================================================================
-- 3. Mark success file arrived only
-- Use this when success marker exists but data file check has not completed.
-- =====================================================================

UPDATE credit_risk_dev.orchestration.file_arrival_status
SET success_file_status = 'ARRIVED',
    arrival_status = 'SUCCESS_ARRIVED',
    actual_success_file_name = expected_success_file_name,
    actual_success_file_path = expected_success_file_path,
    first_success_detected_ts = coalesce(first_success_detected_ts, current_timestamp()),
    last_checked_ts = current_timestamp(),
    poll_count = poll_count + 1,
    updated_timestamp = current_timestamp()
WHERE business_dt = DATE '<business_dt>'
  AND pipeline_run_id = '<pipeline_run_id>'
  AND entity_name = '<entity_name>';

-- COMMAND ----------

-- =====================================================================
-- 4. Mark file as data ready when success file and data file exist
-- =====================================================================

UPDATE credit_risk_dev.orchestration.file_arrival_status
SET success_file_status = 'ARRIVED',
    data_file_status = 'ARRIVED',
    arrival_status = 'DATA_READY',
    actual_success_file_name = expected_success_file_name,
    actual_success_file_path = expected_success_file_path,
    actual_data_file_name = expected_data_file_name,
    actual_data_file_path = expected_data_file_path,
    first_success_detected_ts = coalesce(first_success_detected_ts, current_timestamp()),
    first_data_detected_ts = coalesce(first_data_detected_ts, current_timestamp()),
    last_checked_ts = current_timestamp(),
    poll_count = poll_count + 1,
    updated_timestamp = current_timestamp()
WHERE business_dt = DATE '<business_dt>'
  AND pipeline_run_id = '<pipeline_run_id>'
  AND entity_name = '<entity_name>';

-- COMMAND ----------

-- =====================================================================
-- 5. Mark data file missing after success marker arrived
-- =====================================================================

UPDATE credit_risk_dev.orchestration.file_arrival_status
SET data_file_status = 'MISSING_AFTER_SUCCESS',
    arrival_status = 'FAILED',
    error_message = 'Success marker arrived but matching data file was not found',
    last_checked_ts = current_timestamp(),
    updated_timestamp = current_timestamp()
WHERE business_dt = DATE '<business_dt>'
  AND pipeline_run_id = '<pipeline_run_id>'
  AND entity_name = '<entity_name>';

-- COMMAND ----------

-- =====================================================================
-- 6. Mark SLA missed
-- =====================================================================

UPDATE credit_risk_dev.orchestration.file_arrival_status
SET success_file_status = 'SLA_MISSED',
    data_file_status = 'SLA_MISSED',
    arrival_status = 'SLA_MISSED',
    error_message = 'Success file or data file did not arrive within SLA',
    last_checked_ts = current_timestamp(),
    updated_timestamp = current_timestamp()
WHERE business_dt = DATE '<business_dt>'
  AND pipeline_run_id = '<pipeline_run_id>'
  AND entity_name = '<entity_name>';

-- COMMAND ----------

-- =====================================================================
-- 7. Insert BRONZE running status before Databricks trigger
-- =====================================================================

INSERT INTO credit_risk_dev.orchestration.layer_processing_status
(
    business_dt,
    pipeline_run_id,
    entity_name,
    file_id,
    dependency_group,
    layer_name,
    process_name,
    status,
    databricks_job_name,
    databricks_job_run_id,
    start_timestamp,
    end_timestamp,
    records_read,
    records_written,
    records_rejected,
    retry_count,
    rerun_flag,
    error_message,
    created_timestamp,
    updated_timestamp
)
SELECT
    DATE '<business_dt>' AS business_dt,
    '<pipeline_run_id>' AS pipeline_run_id,
    entity_name,
    file_id,
    dependency_group,
    'BRONZE' AS layer_name,
    'bronze_loader' AS process_name,
    'RUNNING' AS status,
    bronze_silver_job_name AS databricks_job_name,
    CAST(NULL AS STRING) AS databricks_job_run_id,
    current_timestamp() AS start_timestamp,
    CAST(NULL AS TIMESTAMP) AS end_timestamp,
    CAST(NULL AS BIGINT) AS records_read,
    CAST(NULL AS BIGINT) AS records_written,
    CAST(NULL AS BIGINT) AS records_rejected,
    0 AS retry_count,
    false AS rerun_flag,
    CAST(NULL AS STRING) AS error_message,
    current_timestamp() AS created_timestamp,
    current_timestamp() AS updated_timestamp
FROM credit_risk_dev.orchestration.file_ingestion_metadata
WHERE entity_name = '<entity_name>'
  AND active_flag = true;

-- COMMAND ----------

-- =====================================================================
-- 8. Check mandatory Silver Standardization completion
-- If missing_count = 0, ADF can trigger Silver Conformance.
-- =====================================================================

SELECT
    COUNT(*) AS missing_count
FROM credit_risk_dev.orchestration.file_ingestion_metadata m
LEFT JOIN credit_risk_dev.orchestration.layer_processing_status s
  ON m.entity_name = s.entity_name
 AND s.business_dt = DATE '<business_dt>'
 AND s.pipeline_run_id = '<pipeline_run_id>'
 AND s.layer_name = 'SILVER_STANDARDIZATION'
 AND s.status = 'SUCCESS'
WHERE m.active_flag = true
  AND m.mandatory_flag = true
  AND s.entity_name IS NULL;

-- COMMAND ----------

-- =====================================================================
-- 9. Check Gold dependencies
-- If missing_count = 0, ADF can trigger JOB_03_GOLD.
-- =====================================================================

SELECT
    COUNT(*) AS missing_count
FROM credit_risk_dev.orchestration.dependency_rule_metadata r
LEFT JOIN credit_risk_dev.orchestration.layer_processing_status s
  ON r.required_entity_name = s.entity_name
 AND r.required_layer_name = s.layer_name
 AND s.business_dt = DATE '<business_dt>'
 AND s.pipeline_run_id = '<pipeline_run_id>'
 AND s.status = 'SUCCESS'
WHERE r.target_layer_name = 'GOLD'
  AND r.active_flag = true
  AND r.mandatory_flag = true
  AND s.entity_name IS NULL;
