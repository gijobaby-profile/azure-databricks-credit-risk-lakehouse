-- Databricks notebook source
-- =====================================================================
-- Project     : Azure Databricks Credit Risk Lakehouse
-- Module      : ADF Orchestration Metadata
-- File        : 02_insert_pipeline_config_and_file_metadata.sql
-- Purpose     : Insert static pipeline config and Home Credit file-pattern
--               metadata for ADF file watcher orchestration.
-- Notes       :
--   1. Landing path expected:
--        landing/home_credit/{entity_name}/{business_dt}/
--   2. Each data file must have a same-name success marker:
--        <data_file_name>.SUCCESS
--      Example:
--        application_train_part001.csv
--        application_train_part001.csv.SUCCESS
--   3. data_file_name_pattern is intentionally wildcard-based so that
--      multiple files under the same entity/business_dt can be processed.
-- =====================================================================

-- COMMAND ----------

-- ---------------------------------------------------------------------
-- Pipeline configuration
-- ---------------------------------------------------------------------
INSERT INTO credit_risk_dev.orchestration.pipeline_config
(
    pipeline_config_id,
    pipeline_name,
    environment,
    business_dt_rule,
    business_dt_offset_days,
    business_dt_token_format,
    timezone_name,
    allow_manual_business_dt,
    active_flag,
    created_timestamp,
    updated_timestamp
)
VALUES
(
    'CFG_HOME_CREDIT_DEV_001',
    'PL_CREDIT_RISK_ADF_ORCHESTRATOR',
    'dev',
    'CURRENT_DATE_MINUS_N',
    1,
    'yyyyMMdd',
    'Europe/Berlin',
    true,
    true,
    current_timestamp(),
    current_timestamp()
);

-- COMMAND ----------

-- ---------------------------------------------------------------------
-- Home Credit file-pattern metadata
-- ---------------------------------------------------------------------
INSERT INTO credit_risk_dev.orchestration.file_ingestion_metadata
(
    file_id,
    source_system,
    entity_name,
    file_pattern_name,
    source_base_path_pattern,
    data_file_name_pattern,
    success_file_name_suffix,
    file_matching_mode,
    expected_frequency,
    expected_arrival_time,
    sla_grace_minutes,
    min_expected_file_count,
    max_expected_file_count,
    success_file_required_flag,
    mandatory_flag,
    active_flag,
    polling_interval_minutes,
    max_poll_count,
    max_retry_count,
    retry_interval_minutes,
    load_sequence,
    bronze_silver_job_name,
    created_timestamp,
    updated_timestamp
)
VALUES
-- Daily core application files
(
    'FILE_HOME_CREDIT_APPLICATION_TRAIN',
    'home_credit',
    'application_train',
    'application_train_files',
    'landing/home_credit/{entity_name}/{business_dt}/',
    'application_train*.csv',
    '.SUCCESS',
    'WILDCARD',
    'DAILY',
    '06:00:00',
    60,
    1,
    NULL,
    true,
    true,
    true,
    5,
    24,
    2,
    10,
    10,
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_HOME_CREDIT_APPLICATION_TEST',
    'home_credit',
    'application_test',
    'application_test_files',
    'landing/home_credit/{entity_name}/{business_dt}/',
    'application_test*.csv',
    '.SUCCESS',
    'WILDCARD',
    'DAILY',
    '06:10:00',
    60,
    1,
    NULL,
    true,
    false,
    true,
    5,
    24,
    2,
    10,
    20,
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    current_timestamp(),
    current_timestamp()
),

-- Weekly bureau files
(
    'FILE_HOME_CREDIT_BUREAU',
    'home_credit',
    'bureau',
    'bureau_files',
    'landing/home_credit/{entity_name}/{business_dt}/',
    'bureau*.csv',
    '.SUCCESS',
    'WILDCARD',
    'WEEKLY',
    '07:00:00',
    90,
    1,
    NULL,
    true,
    true,
    true,
    5,
    36,
    2,
    10,
    30,
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_HOME_CREDIT_BUREAU_BALANCE',
    'home_credit',
    'bureau_balance',
    'bureau_balance_files',
    'landing/home_credit/{entity_name}/{business_dt}/',
    'bureau_balance*.csv',
    '.SUCCESS',
    'WILDCARD',
    'WEEKLY',
    '07:15:00',
    90,
    1,
    NULL,
    true,
    true,
    true,
    5,
    36,
    2,
    10,
    40,
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_HOME_CREDIT_PREVIOUS_APPLICATION',
    'home_credit',
    'previous_application',
    'previous_application_files',
    'landing/home_credit/{entity_name}/{business_dt}/',
    'previous_application*.csv',
    '.SUCCESS',
    'WILDCARD',
    'WEEKLY',
    '07:30:00',
    90,
    1,
    NULL,
    true,
    true,
    true,
    5,
    36,
    2,
    10,
    50,
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    current_timestamp(),
    current_timestamp()
),

-- Monthly behavioural/payment files
(
    'FILE_HOME_CREDIT_INSTALLMENTS_PAYMENTS',
    'home_credit',
    'installments_payments',
    'installments_payments_files',
    'landing/home_credit/{entity_name}/{business_dt}/',
    'installments_payments*.csv',
    '.SUCCESS',
    'WILDCARD',
    'MONTHLY',
    '08:00:00',
    120,
    1,
    NULL,
    true,
    true,
    true,
    10,
    24,
    2,
    15,
    60,
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_HOME_CREDIT_CREDIT_CARD_BALANCE',
    'home_credit',
    'credit_card_balance',
    'credit_card_balance_files',
    'landing/home_credit/{entity_name}/{business_dt}/',
    'credit_card_balance*.csv',
    '.SUCCESS',
    'WILDCARD',
    'MONTHLY',
    '08:15:00',
    120,
    1,
    NULL,
    true,
    true,
    true,
    10,
    24,
    2,
    15,
    70,
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    current_timestamp(),
    current_timestamp()
),
(
    'FILE_HOME_CREDIT_POS_CASH_BALANCE',
    'home_credit',
    'POS_CASH_balance',
    'pos_cash_balance_files',
    'landing/home_credit/{entity_name}/{business_dt}/',
    'POS_CASH_balance*.csv',
    '.SUCCESS',
    'WILDCARD',
    'MONTHLY',
    '08:30:00',
    120,
    1,
    NULL,
    true,
    true,
    true,
    10,
    24,
    2,
    15,
    80,
    'JOB_01_ENTITY_BRONZE_SILVER_STANDARDIZATION',
    current_timestamp(),
    current_timestamp()
);

