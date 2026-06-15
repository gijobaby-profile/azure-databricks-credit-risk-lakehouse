-- Databricks notebook source
-- =====================================================================
-- File        : 01_create_orchestration_tables.sql
-- Purpose     : Create ADF orchestration metadata and control tables
-- =====================================================================

-- COMMAND ----------

-- =====================================================================
-- 1. PIPELINE CONFIG
-- Purpose: Static pipeline-level configuration for business date logic.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.pipeline_config
(
    pipeline_config_id        STRING COMMENT 'Unique pipeline configuration id',
    pipeline_name             STRING COMMENT 'ADF pipeline name',
    environment               STRING COMMENT 'dev, uat, prod',

    business_dt_rule          STRING COMMENT 'CURRENT_DATE_MINUS_N or MANUAL',
    business_dt_offset_days   INT COMMENT 'Offset days for scheduled runs; 1 means yesterday',
    business_dt_token_format  STRING COMMENT 'Business date token format used in paths and file names, e.g. yyyyMMdd',
    timezone_name             STRING COMMENT 'Business timezone, e.g. Europe/Berlin',
    allow_manual_business_dt  BOOLEAN COMMENT 'Whether manual business_dt override is allowed',

    active_flag               BOOLEAN COMMENT 'Whether this configuration is active',

    created_timestamp         TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp         TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'ADF pipeline-level configuration for business date calculation and environment control';

-- COMMAND ----------

-- =====================================================================
-- 2. FILE INGESTION METADATA
-- Purpose: Static file-pattern level ingestion control.
-- Note   : One row represents one expected file pattern under an entity.
--          A single pattern can match multiple physical files.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.file_ingestion_metadata
(
    file_id                    STRING COMMENT 'Unique file metadata id',
    source_system              STRING COMMENT 'Source system name, e.g. home_credit',
    entity_name                STRING COMMENT 'Logical entity name, e.g. bureau',
    file_pattern_name          STRING COMMENT 'Logical file pattern name under the entity, e.g. bureau_files',

    source_base_path_pattern   STRING COMMENT 'Landing path pattern, e.g. landing/home_credit/{entity_name}/{business_dt}/',
    data_file_name_pattern     STRING COMMENT 'Expected data file pattern, e.g. *.csv or bureau_*.csv',
    success_file_name_suffix   STRING COMMENT 'Success marker suffix, e.g. .SUCCESS',

    file_matching_mode         STRING COMMENT 'EXACT, WILDCARD, PREFIX, REGEX',
    expected_frequency         STRING COMMENT 'DAILY, WEEKLY, MONTHLY, ADHOC',
    expected_arrival_time      STRING COMMENT 'Expected local arrival time in HH:mm:ss format',
    sla_grace_minutes          INT COMMENT 'Grace period after expected arrival time before SLA breach',

    min_expected_file_count    INT COMMENT 'Minimum expected data files for this file pattern',
    max_expected_file_count    INT COMMENT 'Maximum expected data files; null means no upper limit',

    success_file_required_flag BOOLEAN COMMENT 'If true, each data file must have corresponding success marker file',
    mandatory_flag             BOOLEAN COMMENT 'If true, missing file fails the pipeline or dependency',
    active_flag                BOOLEAN COMMENT 'Whether this file pattern is active',

    polling_interval_minutes   INT COMMENT 'ADF polling interval in minutes',
    max_poll_count             INT COMMENT 'Maximum polling attempts before timeout',

    max_retry_count            INT COMMENT 'Maximum controlled retry attempts',
    retry_interval_minutes     INT COMMENT 'Wait time between retry attempts',

    load_sequence              INT COMMENT 'Processing sequence for ADF ForEach loop',
    bronze_silver_job_name     STRING COMMENT 'Databricks job used to process this file to Bronze and Silver Standardization',

    created_timestamp          TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp          TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Static metadata defining expected file patterns, success marker rules, SLA, polling, retry, and Bronze/Silver ingestion control';

-- COMMAND ----------

-- =====================================================================
-- 3. FILE ARRIVAL STATUS
-- Purpose: Runtime summary per business date, pipeline run, and file_id.
--          ADF uses this table to decide whether a file pattern is ready.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.file_arrival_status
(
    business_dt                  DATE COMMENT 'Logical business date',
    pipeline_run_id              STRING COMMENT 'Unique pipeline run id',
    file_id                      STRING COMMENT 'Reference to file_ingestion_metadata.file_id',
    source_system                STRING COMMENT 'Source system name',
    entity_name                  STRING COMMENT 'Logical entity name',
    file_pattern_name            STRING COMMENT 'Logical file pattern name',

    resolved_source_base_path    STRING COMMENT 'Resolved landing path for this business date',
    resolved_data_file_pattern   STRING COMMENT 'Resolved data file pattern used for matching',
    file_matching_mode           STRING COMMENT 'EXACT, WILDCARD, PREFIX, REGEX',

    matched_data_file_count      INT COMMENT 'Number of matched data files',
    matched_success_file_count   INT COMMENT 'Number of matched success marker files',
    missing_success_file_count   INT COMMENT 'Number of data files without corresponding success marker file',

    data_file_arrived_flag       BOOLEAN COMMENT 'Whether minimum required data files arrived',
    success_file_arrived_flag    BOOLEAN COMMENT 'Whether required success marker files arrived',

    total_data_file_size_bytes   BIGINT COMMENT 'Total size of matched data files',

    expected_arrival_timestamp   TIMESTAMP COMMENT 'Expected arrival timestamp for this business date',
    sla_deadline_timestamp       TIMESTAMP COMMENT 'Expected arrival timestamp plus SLA grace period',
    sla_status                   STRING COMMENT 'WITHIN_SLA, SLA_BREACHED, NOT_APPLICABLE',

    arrival_status               STRING COMMENT 'WAITING, DATA_READY, PARTIAL_ARRIVAL, SKIPPED_OPTIONAL, FAILED',
    poll_count                   INT COMMENT 'Number of completed polling attempts',
    retry_count                  INT COMMENT 'Number of controlled retry attempts',
    last_checked_timestamp       TIMESTAMP COMMENT 'Last ADF polling timestamp',

    error_message                STRING COMMENT 'File-arrival summary error message',

    created_timestamp            TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp            TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Runtime summary table tracking file-pattern arrival, wildcard matching, success marker validation, SLA, polling, and retry status';

-- COMMAND ----------

-- =====================================================================
-- 4. FILE ARRIVAL FILE DETAIL
-- Purpose: Runtime physical file-level tracking.
--          One row = one actual data file detected by ADF.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.file_arrival_file_detail
(
    business_dt                  DATE COMMENT 'Logical business date',
    pipeline_run_id              STRING COMMENT 'Unique pipeline run id',
    file_id                      STRING COMMENT 'Reference to file_ingestion_metadata.file_id',
    source_system                STRING COMMENT 'Source system name',
    entity_name                  STRING COMMENT 'Logical entity name',
    file_pattern_name            STRING COMMENT 'Logical file pattern name',

    data_file_name               STRING COMMENT 'Actual data file name',
    data_file_path               STRING COMMENT 'Actual full data file path',
    data_file_size_bytes         BIGINT COMMENT 'Data file size in bytes',
    data_file_last_modified_ts   TIMESTAMP COMMENT 'Data file last modified timestamp from ADLS',

    success_file_name            STRING COMMENT 'Expected or actual success marker file name',
    success_file_path            STRING COMMENT 'Expected or actual success marker full path',
    success_file_arrived_flag    BOOLEAN COMMENT 'Whether matching success marker file arrived',
    success_file_detected_ts     TIMESTAMP COMMENT 'Timestamp when success marker file was detected',

    file_status                  STRING COMMENT 'WAITING_SUCCESS, READY, PROCESSING, PROCESSED, FAILED, SKIPPED, DUPLICATE',

    detected_timestamp           TIMESTAMP COMMENT 'Timestamp when ADF detected this data file',
    error_message                STRING COMMENT 'File-level error message',

    created_timestamp            TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp            TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Runtime detail table storing physical data files, matching success files, and file-level readiness status';

-- COMMAND ----------

-- =====================================================================
-- 5. LAYER PROCESSING STATUS
-- Purpose: Runtime Databricks layer/job status tracking.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.layer_processing_status
(
    business_dt                  DATE COMMENT 'Logical business date',
    pipeline_run_id              STRING COMMENT 'Unique pipeline run id',

    source_system                STRING COMMENT 'Source system name',
    entity_name                  STRING COMMENT 'Logical entity name',
    file_pattern_name            STRING COMMENT 'Optional file pattern name for file-level processing',
    data_file_name               STRING COMMENT 'Optional physical file name for file-level processing',

    layer_name                   STRING COMMENT 'BRONZE, SILVER_STANDARDIZATION, SILVER_CONFORMANCE, GOLD',
    process_name                 STRING COMMENT 'Process or notebook name',
    status                       STRING COMMENT 'NOT_STARTED, RUNNING, SUCCESS, FAILED, SKIPPED',

    databricks_job_run_id        STRING COMMENT 'Databricks job run id',

    start_timestamp              TIMESTAMP COMMENT 'Process start timestamp',
    end_timestamp                TIMESTAMP COMMENT 'Process end timestamp',

    records_read                 BIGINT COMMENT 'Records read',
    records_written              BIGINT COMMENT 'Records written',
    records_rejected             BIGINT COMMENT 'Records rejected',

    retry_count                  INT COMMENT 'Number of process retry attempts',
    force_reload                 BOOLEAN COMMENT 'Whether this process used force_reload',

    error_message                STRING COMMENT 'Layer or process error message',

    created_timestamp            TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp            TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Runtime layer-level processing status for file-level ingestion, Silver Conformance, and Gold dependency checks';

-- COMMAND ----------

-- =====================================================================
-- 6. DEPENDENCY RULE METADATA
-- Purpose: Static dependency rules for Silver Conformance and Gold.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.dependency_rule_metadata
(
    dependency_rule_id           STRING COMMENT 'Unique dependency rule id',

    target_layer_name            STRING COMMENT 'Target layer, e.g. SILVER_CONFORMANCE or GOLD',
    target_process_name          STRING COMMENT 'Target process name, e.g. conformed_loan_application or gold_risk_mart',
    target_entity_name           STRING COMMENT 'Target entity or process name',

    required_layer_name          STRING COMMENT 'Required predecessor layer',
    required_entity_name         STRING COMMENT 'Required predecessor entity',
    required_file_pattern_name   STRING COMMENT 'Optional required file pattern; null means all required patterns for the entity',
    required_status              STRING COMMENT 'Required predecessor status, normally SUCCESS',

    mandatory_flag               BOOLEAN COMMENT 'If true, dependency must be successful',
    active_flag                  BOOLEAN COMMENT 'Whether this dependency rule is active',

    created_timestamp            TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp            TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Static dependency rules used by ADF to control Silver Conformance and Gold execution';

-- COMMAND ----------

-- =====================================================================
-- 7. PIPELINE RUN CONTROL
-- Purpose: One row per ADF orchestration run.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.pipeline_run_control
(
    business_dt                  DATE COMMENT 'Logical business date',
    pipeline_run_id              STRING COMMENT 'Unique generated pipeline run id',
    adf_run_id                   STRING COMMENT 'ADF native run id',
    pipeline_name                STRING COMMENT 'ADF pipeline name',
    environment                  STRING COMMENT 'dev, uat, prod',

    run_scope                    STRING COMMENT 'FULL, ENTITY, FILE, LAYER',
    run_type                     STRING COMMENT 'SCHEDULED, MANUAL, RERUN, BACKDATED, CATCHUP',

    source_system                STRING COMMENT 'Source system name',
    entity_name                  STRING COMMENT 'Optional entity name for scoped run',
    file_pattern_name            STRING COMMENT 'Optional file pattern name for scoped run',
    data_file_name               STRING COMMENT 'Optional physical file name for file-level rerun',
    layer_name                   STRING COMMENT 'Optional layer name for layer-level rerun',

    force_reload                 BOOLEAN COMMENT 'Whether this run reloads already processed data',
    overall_status               STRING COMMENT 'RUNNING, SUCCESS, FAILED, PARTIAL_SUCCESS, CANCELLED',

    start_timestamp              TIMESTAMP COMMENT 'Pipeline start timestamp',
    end_timestamp                TIMESTAMP COMMENT 'Pipeline end timestamp',

    requested_by                 STRING COMMENT 'User or system that triggered the run',
    error_message                STRING COMMENT 'Pipeline-level error message',

    created_timestamp            TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp            TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Master runtime control table for scheduled, manual, backdated, and file-level ADF orchestration runs';

-- COMMAND ----------

-- =====================================================================
-- 8. RERUN REQUEST CONTROL
-- Purpose: Controlled manual/backdated/entity/file/layer rerun requests.
-- =====================================================================
CREATE TABLE IF NOT EXISTS credit_risk_dev.orchestration.rerun_request_control
(
    rerun_request_id             STRING COMMENT 'Unique rerun request id',
    request_status               STRING COMMENT 'REQUESTED, APPROVED, REJECTED, IN_PROGRESS, COMPLETED, FAILED',

    business_dt                  DATE COMMENT 'Business date to rerun',
    run_scope                    STRING COMMENT 'FULL, ENTITY, FILE, LAYER',
    run_type                     STRING COMMENT 'RERUN, BACKDATED, CATCHUP, MANUAL',

    source_system                STRING COMMENT 'Source system name',
    entity_name                  STRING COMMENT 'Optional entity name for rerun',
    file_pattern_name            STRING COMMENT 'Optional file pattern name for rerun',
    data_file_name               STRING COMMENT 'Optional physical data file name for file-level rerun',
    layer_name                   STRING COMMENT 'Optional layer name for layer-level rerun',

    force_reload                 BOOLEAN COMMENT 'Whether to reload already processed data',
    rerun_reason                 STRING COMMENT 'Business or technical reason for rerun',

    requested_by                 STRING COMMENT 'Requester',
    approved_by                  STRING COMMENT 'Approver',

    requested_timestamp          TIMESTAMP COMMENT 'Request timestamp',
    approved_timestamp           TIMESTAMP COMMENT 'Approval timestamp',

    executed_pipeline_run_id     STRING COMMENT 'Pipeline run id created for this rerun',
    error_message                STRING COMMENT 'Rerun request error message',

    created_timestamp            TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp            TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Controlled rerun, backdated, catch-up, entity-level, file-level, and layer-level request table';
