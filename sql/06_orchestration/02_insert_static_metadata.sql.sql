-- Databricks notebook source
-- =====================================================================
-- File        : 02_insert_static_metadata.sql
-- Purpose     : Insert static orchestration metadata
-- =====================================================================

-- COMMAND ----------

-- =====================================================================
-- 1. Pipeline config
-- =====================================================================
DELETE FROM credit_risk_dev.orchestration.pipeline_config
WHERE pipeline_config_id = 'CFG_CREDIT_RISK_DAILY_001';

INSERT INTO credit_risk_dev.orchestration.pipeline_config
SELECT
    'CFG_CREDIT_RISK_DAILY_001'       AS pipeline_config_id,
    'PL_CREDIT_RISK_ADF_ORCHESTRATOR' AS pipeline_name,
    'dev'                             AS environment,

    'CURRENT_DATE_MINUS_N'            AS business_dt_calculation_rule,
    1                                 AS business_dt_offset_days,
    'yyyyMMdd'                        AS business_dt_format,
    'Europe/Berlin'                   AS timezone_name,

    true                              AS allow_manual_business_dt,
    true                              AS active_flag,

    current_timestamp()               AS created_timestamp,
    current_timestamp()               AS updated_timestamp;

-- COMMAND ----------

-- =====================================================================
-- 2. File ingestion metadata
-- =====================================================================
DELETE FROM credit_risk_dev.orchestration.file_ingestion_metadata
WHERE source_system = 'home_credit';

INSERT INTO credit_risk_dev.orchestration.file_ingestion_metadata
SELECT * FROM VALUES
(
    'FILE_APPLICATION_TRAIN',
    'home_credit',
    'application_train',
    'abfss://landing@stcrcurateddevuks001.dfs.core.windows.net/home_credit/application_train/',
    'application_train_${business_dt_yyyyMMdd}.csv',
    'application_train_${business_dt_yyyyMMdd}.success',
    true,
    'DAILY',
    '06:00',
    30,
    5,
    24,
    true,
    true,
    'customer',
    'customer_scd',
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    'JOB_02A_CUSTOMER_SCD_CONFORMANCE',
    'credit_risk_dev.bronze.application_train',
    'credit_risk_dev.silver.standardized_application_train',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_APPLICATION_TEST',
    'home_credit',
    'application_test',
    'abfss://landing@stcrcurateddevuks001.dfs.core.windows.net/home_credit/application_test/',
    'application_test_${business_dt_yyyyMMdd}.csv',
    'application_test_${business_dt_yyyyMMdd}.success',
    true,
    'DAILY',
    '06:05',
    30,
    5,
    24,
    false,
    true,
    'customer',
    'none',
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    null,
    'credit_risk_dev.bronze.application_test',
    'credit_risk_dev.silver.standardized_application_test',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_BUREAU',
    'home_credit',
    'bureau',
    'abfss://landing@stcrcurateddevuks001.dfs.core.windows.net/home_credit/bureau/',
    'bureau_${business_dt_yyyyMMdd}.csv',
    'bureau_${business_dt_yyyyMMdd}.success',
    true,
    'DAILY',
    '06:30',
    45,
    5,
    24,
    true,
    true,
    'bureau',
    'generic_entity',
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    'JOB_02B_ENTITY_CONFORMANCE',
    'credit_risk_dev.bronze.bureau',
    'credit_risk_dev.silver.standardized_bureau',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_BUREAU_BALANCE',
    'home_credit',
    'bureau_balance',
    'abfss://landing@stcrcurateddevuks001.dfs.core.windows.net/home_credit/bureau_balance/',
    'bureau_balance_${business_dt_yyyyMMdd}.csv',
    'bureau_balance_${business_dt_yyyyMMdd}.success',
    true,
    'DAILY',
    '06:45',
    45,
    5,
    24,
    true,
    true,
    'bureau',
    'generic_entity',
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    'JOB_02B_ENTITY_CONFORMANCE',
    'credit_risk_dev.bronze.bureau_balance',
    'credit_risk_dev.silver.standardized_bureau_balance',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_PREVIOUS_APPLICATION',
    'home_credit',
    'previous_application',
    'abfss://landing@stcrcurateddevuks001.dfs.core.windows.net/home_credit/previous_application/',
    'previous_application_${business_dt_yyyyMMdd}.csv',
    'previous_application_${business_dt_yyyyMMdd}.success',
    true,
    'DAILY',
    '07:00',
    60,
    5,
    24,
    true,
    true,
    'loan',
    'generic_entity',
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    'JOB_02B_ENTITY_CONFORMANCE',
    'credit_risk_dev.bronze.previous_application',
    'credit_risk_dev.silver.standardized_previous_application',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_INSTALLMENTS_PAYMENTS',
    'home_credit',
    'installments_payments',
    'abfss://landing@stcrcurateddevuks001.dfs.core.windows.net/home_credit/installments_payments/',
    'installments_payments_${business_dt_yyyyMMdd}.csv',
    'installments_payments_${business_dt_yyyyMMdd}.success',
    true,
    'DAILY',
    '07:30',
    60,
    5,
    24,
    true,
    true,
    'payment',
    'generic_entity',
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    'JOB_02B_ENTITY_CONFORMANCE',
    'credit_risk_dev.bronze.installments_payments',
    'credit_risk_dev.silver.standardized_installments_payments',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_CREDIT_CARD_BALANCE',
    'home_credit',
    'credit_card_balance',
    'abfss://landing@stcrcurateddevuks001.dfs.core.windows.net/home_credit/credit_card_balance/',
    'credit_card_balance_${business_dt_yyyyMMdd}.csv',
    'credit_card_balance_${business_dt_yyyyMMdd}.success',
    true,
    'DAILY',
    '08:00',
    60,
    5,
    24,
    false,
    true,
    'card',
    'generic_entity',
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    'JOB_02B_ENTITY_CONFORMANCE',
    'credit_risk_dev.bronze.credit_card_balance',
    'credit_risk_dev.silver.standardized_credit_card_balance',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_POS_CASH_BALANCE',
    'home_credit',
    'pos_cash_balance',
    'abfss://landing@stcrcurateddevuks001.dfs.core.windows.net/home_credit/pos_cash_balance/',
    'pos_cash_balance_${business_dt_yyyyMMdd}.csv',
    'pos_cash_balance_${business_dt_yyyyMMdd}.success',
    true,
    'DAILY',
    '08:15',
    60,
    5,
    24,
    false,
    true,
    'payment',
    'generic_entity',
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    'JOB_02B_ENTITY_CONFORMANCE',
    'credit_risk_dev.bronze.pos_cash_balance',
    'credit_risk_dev.silver.standardized_pos_cash_balance',
    current_timestamp(),
    current_timestamp()
)
AS t
(
    file_id,
    source_system,
    entity_name,
    source_base_path,
    data_file_name_pattern,
    success_file_name_pattern,
    success_file_required_flag,
    expected_frequency,
    expected_arrival_time,
    sla_grace_minutes,
    polling_interval_minutes,
    max_poll_count,
    mandatory_flag,
    active_flag,
    dependency_group,
    conformance_group,
    bronze_silver_job_name,
    conformance_job_name,
    target_bronze_table,
    target_silver_table,
    created_timestamp,
    updated_timestamp
);

-- COMMAND ----------

-- =====================================================================
-- PHASE 2 OPTIONAL SAMPLE METADATA
-- =====================================================================
-- This sample is optional. Keep it commented until you start using
-- rerun_request_control operationally.
-- =====================================================================

 INSERT INTO credit_risk_dev.orchestration.rerun_request_control
 SELECT
     'RERUN_20260607_APPLICATION_TRAIN_001' AS rerun_request_id,
     'APPROVED' AS request_status,
     current_user() AS requested_by,
     current_user() AS approved_by,
     'RERUN' AS run_type,
     DATE '2026-06-07' AS business_dt,
     'application_train' AS entity_name,
     'FILE_APPLICATION_TRAIN' AS file_id,
     'BRONZE' AS layer_name,
     CAST(NULL AS STRING) AS override_data_file_path,
     CAST(NULL AS STRING) AS override_success_file_path,
     true AS force_reload,
     'Manual rerun due to corrected source file received after SLA' AS rerun_reason,
     current_timestamp() AS requested_timestamp,
     current_timestamp() AS approved_timestamp,
     CAST(NULL AS TIMESTAMP) AS start_timestamp,
     CAST(NULL AS TIMESTAMP) AS end_timestamp,
     CAST(NULL AS STRING) AS error_message,
     current_timestamp() AS created_timestamp,
     current_timestamp() AS updated_timestamp;
