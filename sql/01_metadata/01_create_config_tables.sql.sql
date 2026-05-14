-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create metadata/configuration tables

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.config.bronze_ingestion_config (
    entity_name STRING COMMENT 'Source entity or file name, for example application_train',
    source_system STRING COMMENT 'Source system name, for example home_credit',
    source_path STRING COMMENT 'Source volume path or relative path',
    target_table_full_name STRING COMMENT 'Fully qualified Bronze target table name',
    file_format STRING COMMENT 'Input file format, for example csv',
    delimiter STRING COMMENT 'File delimiter',
    header_flag BOOLEAN COMMENT 'Whether the file contains header row',
    infer_schema_flag BOOLEAN COMMENT 'Whether schema inference is enabled',
    merge_schema_flag BOOLEAN COMMENT 'Whether schema evolution is enabled',
    load_enabled BOOLEAN COMMENT 'Whether this entity should be loaded',
    load_sequence INT COMMENT 'Execution order for loading entities'
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.config.silver_column_config (
    entity_name STRING COMMENT 'Source entity name',
    source_column_name STRING COMMENT 'Column name from Bronze table',
    source_column_position INT COMMENT 'Column position in source file',
    target_column_name STRING COMMENT 'Standardized Silver column name',
    target_data_type STRING COMMENT 'Target datatype in Silver table',
    is_required BOOLEAN COMMENT 'Whether the column is mandatory',
    is_dedup_key BOOLEAN COMMENT 'Whether the column is used for duplicate removal',
    is_active BOOLEAN COMMENT 'Whether this column mapping is active',
    column_sequence INT COMMENT 'Column order in standardized output'
)
USING DELTA;

-- COMMAND ----------


