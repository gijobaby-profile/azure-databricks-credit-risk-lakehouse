-- Databricks notebook source
-- File        : 05_validation_queries.sql
-- Purpose     : Validation and monitoring queries

-- COMMAND ----------

select * from credit_risk_dev.orchestration.file_ingestion_metadata

-- COMMAND ----------

-- Show orchestration tables
SHOW TABLES IN credit_risk_dev.orchestration;

-- COMMAND ----------

-- Validate active pipeline config
SELECT *
FROM credit_risk_dev.orchestration.pipeline_config
WHERE active_flag = true;

-- COMMAND ----------

-- Validate file metadata
SELECT
    file_id,
    entity_name,
    source_base_path,
    data_file_name_pattern,
    success_file_name_pattern,
    expected_arrival_time,
    sla_grace_minutes,
    mandatory_flag,
    dependency_group,
    conformance_group
FROM credit_risk_dev.orchestration.file_ingestion_metadata
WHERE active_flag = true
ORDER BY expected_arrival_time, entity_name;

-- COMMAND ----------

-- Validate dependency rules
SELECT *
FROM credit_risk_dev.orchestration.dependency_rule_metadata
WHERE active_flag = true
ORDER BY target_layer_name, target_process_name, required_entity_name;

-- COMMAND ----------

-- Check file status for a business date
SELECT
    business_dt,
    pipeline_run_id,
    entity_name,
    expected_data_file_name,
    expected_success_file_name,
    expected_data_file_path,
    expected_success_file_path,
    success_file_status,
    data_file_status,
    arrival_status,
    poll_count,
    mandatory_flag,
    error_message
FROM credit_risk_dev.orchestration.file_arrival_status
WHERE business_dt = DATE '<business_dt>'
ORDER BY entity_name;

-- COMMAND ----------

-- Check layer status for a business date
SELECT
    business_dt,
    pipeline_run_id,
    entity_name,
    layer_name,
    process_name,
    status,
    start_timestamp,
    end_timestamp,
    records_written,
    records_rejected,
    error_message
FROM credit_risk_dev.orchestration.layer_processing_status
WHERE business_dt = DATE '<business_dt>'
ORDER BY layer_name, entity_name;

-- COMMAND ----------

-- Summary: file arrival by status
SELECT
    business_dt,
    pipeline_run_id,
    arrival_status,
    COUNT(*) AS file_count
FROM credit_risk_dev.orchestration.file_arrival_status
WHERE business_dt = DATE '<business_dt>'
GROUP BY business_dt, pipeline_run_id, arrival_status
ORDER BY arrival_status;

-- COMMAND ----------

-- Summary: layer status by layer
SELECT
    business_dt,
    pipeline_run_id,
    layer_name,
    status,
    COUNT(*) AS status_count
FROM credit_risk_dev.orchestration.layer_processing_status
WHERE business_dt = DATE '<business_dt>'
GROUP BY business_dt, pipeline_run_id, layer_name, status
ORDER BY layer_name, status;

-- COMMAND ----------

-- Check whether all mandatory Silver Standardization entities are complete
SELECT
    COUNT(*) AS missing_mandatory_silver_count
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

-- pipeline run dashboard
SELECT *
FROM credit_risk_dev.orchestration.pipeline_run_control
WHERE business_dt = DATE '<business_dt>'
ORDER BY start_timestamp DESC;

-- COMMAND ----------

-- rerun requests
SELECT *
FROM credit_risk_dev.orchestration.rerun_request_control
WHERE business_dt = DATE '<business_dt>'
ORDER BY requested_timestamp DESC;
