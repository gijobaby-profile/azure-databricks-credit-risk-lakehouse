-- Databricks notebook source
-- =====================================================================
-- File        : 01_create_orchestration_tables.sql
-- Purpose     : Create ADF orchestration metadata and control tables
-- =====================================================================

-- COMMAND ----------

-- =====================================================================
-- 1. pipeline_config
-- Static pipeline configuration used by ADF to calculate business_dt.
-- This table stores the RULE only. The actual business_dt is calculated
-- at runtime by ADF and stored in runtime status tables.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.pipeline_config
(
    pipeline_config_id              STRING COMMENT 'Unique pipeline configuration id',
    pipeline_name                   STRING COMMENT 'ADF pipeline name',
    environment                     STRING COMMENT 'dev, uat, prod',

    business_dt_calculation_rule     STRING COMMENT 'CURRENT_DATE, CURRENT_DATE_MINUS_N, LAST_DAY_PREVIOUS_MONTH, MANUAL',
    business_dt_offset_days          INT COMMENT 'Offset for CURRENT_DATE_MINUS_N rule',
    business_dt_format               STRING COMMENT 'Business date token format used in file names, e.g. yyyyMMdd',
    timezone_name                    STRING COMMENT 'Business timezone used by ADF, e.g. Europe/Berlin',

    allow_manual_business_dt         BOOLEAN COMMENT 'Allows manual business_dt override from ADF parameter',
    active_flag                      BOOLEAN COMMENT 'Whether this configuration is active',

    created_timestamp                TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp                TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Static pipeline-level configuration used by ADF to derive business_dt';


-- COMMAND ----------


-- =====================================================================
-- 2. file_ingestion_metadata
-- Static metadata defining expected data files and success marker files.
--
-- Important:
-- source_base_path must match your real landing location.
-- Example ABFSS path:
--   abfss://landing@stcrcurateddevuks001.dfs.core.windows.net/home_credit/application_train/
--
-- Example UC Volume path:
--   /Volumes/credit_risk_dev/files/vol_landing_home_credit_dev/application_train/
--
-- Use the same style expected by your Bronze COPY INTO implementation.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.file_ingestion_metadata
(
    file_id                         STRING COMMENT 'Unique file metadata id',
    source_system                   STRING COMMENT 'Source system name, e.g. home_credit',
    entity_name                     STRING COMMENT 'Logical entity name, e.g. application_train',

    source_base_path                STRING COMMENT 'Landing folder where data and success files are expected',
    data_file_name_pattern          STRING COMMENT 'Data file pattern using ${business_dt_yyyyMMdd}',
    success_file_name_pattern       STRING COMMENT 'Success marker pattern using ${business_dt_yyyyMMdd}',
    success_file_required_flag      BOOLEAN COMMENT 'If true, ADF waits for success file before processing data file',

    expected_frequency              STRING COMMENT 'DAILY, MONTHLY, WEEKLY, ADHOC',
    expected_arrival_time           STRING COMMENT 'Expected arrival time in HH:mm',
    sla_grace_minutes               INT COMMENT 'Additional waiting time after expected arrival',
    polling_interval_minutes        INT COMMENT 'ADF wait interval between polling attempts',
    max_poll_count                  INT COMMENT 'Maximum number of polling attempts',

    mandatory_flag                  BOOLEAN COMMENT 'If true, downstream processing cannot continue if missing',
    active_flag                     BOOLEAN COMMENT 'Whether this file metadata is active',

    dependency_group                STRING COMMENT 'customer, bureau, loan, payment, card, all_mandatory',
    conformance_group               STRING COMMENT 'customer_scd, generic_entity, none',

    bronze_silver_job_name          STRING COMMENT 'Databricks job for Bronze and Silver Standardization',
    conformance_job_name            STRING COMMENT 'Databricks job for Silver Conformance, if applicable',

    target_bronze_table             STRING COMMENT 'Fully qualified Bronze target table',
    target_silver_table             STRING COMMENT 'Fully qualified Silver standardized target table',

    created_timestamp               TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp               TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Static metadata defining expected source files, success markers, SLA and Databricks processing configuration';

-- COMMAND ----------

-- =====================================================================
-- 3. file_arrival_status
-- Runtime ADF polling status per business_dt, pipeline_run_id and entity.
--
-- ADF creates one row per expected file for a business_dt.
-- ADF waits for success marker file first. Once success arrives, it checks
-- the actual data file and then triggers Databricks.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.file_arrival_status
(
    business_dt                     DATE COMMENT 'Logical business date',
    pipeline_run_id                 STRING COMMENT 'ADF pipeline run id',
    file_id                         STRING COMMENT 'Reference to file_ingestion_metadata.file_id',
    source_system                   STRING COMMENT 'Source system name',
    entity_name                     STRING COMMENT 'Logical entity name',

    expected_data_file_name         STRING COMMENT 'Resolved expected data file name',
    expected_success_file_name      STRING COMMENT 'Resolved expected success marker file name',
    expected_data_file_path         STRING COMMENT 'Resolved expected data file path',
    expected_success_file_path      STRING COMMENT 'Resolved expected success marker path',

    actual_data_file_name           STRING COMMENT 'Actual detected data file name',
    actual_success_file_name        STRING COMMENT 'Actual detected success marker file name',
    actual_data_file_path           STRING COMMENT 'Actual detected data file path',
    actual_success_file_path        STRING COMMENT 'Actual detected success marker path',

    sla_deadline_ts                 TIMESTAMP COMMENT 'Final SLA deadline timestamp',
    first_success_detected_ts       TIMESTAMP COMMENT 'First timestamp when success marker was detected',
    first_data_detected_ts          TIMESTAMP COMMENT 'First timestamp when data file was detected',
    last_checked_ts                 TIMESTAMP COMMENT 'Last ADF polling check timestamp',

    success_file_status             STRING COMMENT 'EXPECTED, WAITING, ARRIVED, SLA_MISSED, NOT_REQUIRED',
    data_file_status                STRING COMMENT 'EXPECTED, WAITING, ARRIVED, MISSING_AFTER_SUCCESS, SLA_MISSED',
    arrival_status                  STRING COMMENT 'WAITING_FOR_SUCCESS, SUCCESS_ARRIVED, DATA_READY, SLA_MISSED, FAILED',

    mandatory_flag                  BOOLEAN COMMENT 'Whether the file is mandatory',
    poll_count                      INT COMMENT 'Number of polling attempts completed',

    bronze_silver_trigger_status    STRING COMMENT 'NOT_TRIGGERED, TRIGGERED, RUNNING, SUCCESS, FAILED',
    databricks_job_run_id           STRING COMMENT 'Databricks job run id returned to ADF',

    error_message                   STRING COMMENT 'File-arrival or trigger-level error message',

    created_timestamp               TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp               TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Runtime status table tracking success marker and data file arrival';

-- COMMAND ----------

-- =====================================================================
-- 4. layer_processing_status
-- Runtime status by business_dt, entity/process and processing layer.
-- Used by ADF to decide when Conformance and Gold can start.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.layer_processing_status
(
    business_dt                     DATE COMMENT 'Logical business date',
    pipeline_run_id                 STRING COMMENT 'ADF pipeline run id',
    entity_name                     STRING COMMENT 'Entity name or process name such as GOLD_RISK_MART',
    file_id                         STRING COMMENT 'Reference to file metadata, if applicable',
    dependency_group                STRING COMMENT 'Dependency group',

    layer_name                      STRING COMMENT 'BRONZE, SILVER_STANDARDIZATION, SILVER_CONFORMANCE, GOLD',
    process_name                    STRING COMMENT 'bronze_loader, silver_standardizer, customer_scd, generic_conformance, gold_risk_mart',
    status                          STRING COMMENT 'NOT_STARTED, WAITING, RUNNING, SUCCESS, FAILED, SKIPPED, RERUN_REQUIRED',

    databricks_job_name             STRING COMMENT 'Databricks job name',
    databricks_job_run_id           STRING COMMENT 'Databricks job run id',

    start_timestamp                 TIMESTAMP COMMENT 'Layer/process start timestamp',
    end_timestamp                   TIMESTAMP COMMENT 'Layer/process end timestamp',

    records_read                    BIGINT COMMENT 'Records read by the layer/process',
    records_written                 BIGINT COMMENT 'Records written by the layer/process',
    records_rejected                BIGINT COMMENT 'Records rejected by the layer/process',

    retry_count                     INT COMMENT 'Number of retries',
    rerun_flag                      BOOLEAN COMMENT 'Whether this status row belongs to a rerun',
    error_message                   STRING COMMENT 'Layer/process error message',

    created_timestamp               TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp               TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
PARTITIONED BY (business_dt, layer_name)
COMMENT 'Runtime layer-level processing status used by ADF for dependency checks';

-- COMMAND ----------

-- =====================================================================
-- 5. dependency_rule_metadata
-- Static dependency rules to decide when Conformance and Gold can start.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.dependency_rule_metadata
(
    dependency_rule_id              STRING COMMENT 'Unique dependency rule id',

    target_layer_name               STRING COMMENT 'SILVER_CONFORMANCE or GOLD',
    target_process_name             STRING COMMENT 'customer_scd, generic_entity_conformance, gold_risk_mart',
    target_entity_name              STRING COMMENT 'Target entity/process name',

    dependency_group                STRING COMMENT 'customer, bureau, loan, payment, card, all_mandatory',
    required_entity_name            STRING COMMENT 'Required upstream entity/process',
    required_layer_name             STRING COMMENT 'SILVER_STANDARDIZATION or SILVER_CONFORMANCE',

    mandatory_flag                  BOOLEAN COMMENT 'If true, target cannot start unless dependency succeeds',
    active_flag                     BOOLEAN COMMENT 'Whether this dependency rule is active',

    created_timestamp               TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp               TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Static dependency rules used by ADF to decide when Conformance and Gold can start';

-- COMMAND ----------

-- =====================================================================
-- PHASE 2 OPTIONAL TABLES
-- =====================================================================
-- These tables are included in this file for completeness.
-- You can create them now, but the first ADF implementation can run without
-- actively using them.
-- =====================================================================


-- =====================================================================
-- 6. pipeline_run_control
-- Optional run-level summary table for business-date dashboard.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.pipeline_run_control
(
    business_dt                     DATE COMMENT 'Logical business date being processed',
    pipeline_run_id                 STRING COMMENT 'Unique ADF or generated pipeline run id',
    adf_pipeline_name               STRING COMMENT 'ADF pipeline name',
    adf_run_id                      STRING COMMENT 'ADF run id',
    environment                     STRING COMMENT 'dev, uat, prod',

    run_type                        STRING COMMENT 'SCHEDULED, MANUAL, RERUN, CATCHUP',
    business_dt_source              STRING COMMENT 'AUTO, MANUAL, RERUN_REQUEST',
    final_business_dt               DATE COMMENT 'Final business date selected for processing',

    overall_status                  STRING COMMENT 'STARTED, RUNNING, SUCCESS, FAILED, PARTIAL_SUCCESS, SLA_MISSED',

    start_timestamp                 TIMESTAMP COMMENT 'Pipeline start timestamp',
    end_timestamp                   TIMESTAMP COMMENT 'Pipeline end timestamp',

    rerun_flag                      BOOLEAN COMMENT 'Whether this run is a rerun/catch-up',
    rerun_reason                    STRING COMMENT 'Rerun reason when applicable',
    error_message                   STRING COMMENT 'Run-level error message',

    created_timestamp               TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp               TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Optional master runtime control table for ADF business-date run monitoring';

-- COMMAND ----------

-- =====================================================================
-- 7. rerun_request_control
-- Optional rerun/catch-up request table for controlled manual operations.
-- In Phase 1, rerun can be handled using ADF parameters directly.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.rerun_request_control
(
    rerun_request_id                STRING COMMENT 'Unique rerun request id',
    request_status                  STRING COMMENT 'REQUESTED, APPROVED, IN_PROGRESS, COMPLETED, REJECTED, FAILED',

    requested_by                    STRING COMMENT 'User who requested rerun',
    approved_by                     STRING COMMENT 'User who approved rerun',

    run_type                        STRING COMMENT 'RERUN, CATCHUP, MANUAL',
    business_dt                     DATE COMMENT 'Business date to rerun',

    entity_name                     STRING COMMENT 'Optional entity for entity-level rerun',
    file_id                         STRING COMMENT 'Optional file_id for file-level rerun',
    layer_name                      STRING COMMENT 'Optional layer name: BRONZE, SILVER_STANDARDIZATION, SILVER_CONFORMANCE, GOLD',

    override_data_file_path         STRING COMMENT 'Optional manual data file path override',
    override_success_file_path      STRING COMMENT 'Optional manual success file path override',

    force_reload                    BOOLEAN COMMENT 'Whether Databricks should force reload source files',
    rerun_reason                    STRING COMMENT 'Business or technical rerun reason',

    requested_timestamp             TIMESTAMP COMMENT 'Request timestamp',
    approved_timestamp              TIMESTAMP COMMENT 'Approval timestamp',
    start_timestamp                 TIMESTAMP COMMENT 'Rerun start timestamp',
    end_timestamp                   TIMESTAMP COMMENT 'Rerun end timestamp',

    error_message                   STRING COMMENT 'Rerun request error message',

    created_timestamp               TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp               TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Optional manual rerun and catch-up control table';
