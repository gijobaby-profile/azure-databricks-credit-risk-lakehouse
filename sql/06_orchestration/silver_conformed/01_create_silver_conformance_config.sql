-- 01_create_silver_conformance_config.sql
-- Purpose: Configuration table to drive Silver Conformed layer dynamically

CREATE TABLE IF NOT EXISTS credit_risk_dev.config.silver_conformance_config (
    conformance_id        STRING COMMENT 'Unique identifier for the silver conformed job',
    source_entity_name    STRING COMMENT 'Source silver standardized entity name',
    target_entity_name    STRING COMMENT 'Target silver conformed entity name',
    target_table_name     STRING COMMENT 'Fully qualified target silver conformed table',
    notebook_path         STRING COMMENT 'Databricks notebook path used for processing',
    load_order            INT COMMENT 'Execution order for dependency sequencing',
    is_active             BOOLEAN COMMENT 'Whether this conformance job is active',
    created_ts            TIMESTAMP COMMENT 'Record creation timestamp',
    updated_ts            TIMESTAMP COMMENT 'Record update timestamp'
)
USING DELTA;