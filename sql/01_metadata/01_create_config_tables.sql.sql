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

-- MAGIC %md
-- MAGIC #### Silver Conformance layer ( reads and prepares the base source data )

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.config.silver_conformance_entity_config (
    entity_name STRING COMMENT 'Conformed entity name, for example loan_application',
    target_catalog_name STRING COMMENT 'Target catalog name',
    target_schema_name STRING COMMENT 'Target schema name',
    target_table_name STRING COMMENT 'Target table name',
    source_query STRING COMMENT 'SQL query used to build the conformed source DataFrame',
    load_strategy STRING COMMENT 'SCD2, REPLACE_BUSINESS_DT, MERGE, or OVERWRITE',
    business_key_columns ARRAY<STRING> COMMENT 'Business key columns used for merge/deduplication',
    hash_columns ARRAY<STRING> COMMENT 'Columns used to create record_hash',
    effective_timestamp_column STRING COMMENT 'Timestamp column used as effective_from for SCD2',
    is_scd2 BOOLEAN COMMENT 'Whether the entity uses SCD Type 2',
    load_enabled BOOLEAN COMMENT 'Whether this entity is enabled for processing',
    load_sequence INT COMMENT 'Entity processing order',
    created_timestamp TIMESTAMP COMMENT 'Config creation timestamp',
    updated_timestamp TIMESTAMP COMMENT 'Config update timestamp'
)
USING DELTA
COMMENT 'Metadata configuration for Silver Conformance business entities';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Silver Conformance layer ( applies derived/calculated columns )

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.config.silver_conformance_derived_column_config (
    entity_name STRING COMMENT 'Conformed entity name',
    derived_column_name STRING COMMENT 'Derived column name',
    derived_sql_expression STRING COMMENT 'Spark SQL expression used to calculate derived column',
    target_data_type STRING COMMENT 'Target datatype for derived column',
    is_active BOOLEAN COMMENT 'Whether derived column is active',
    column_sequence INT COMMENT 'Execution/order sequence',
    created_timestamp TIMESTAMP COMMENT 'Config creation timestamp',
    updated_timestamp TIMESTAMP COMMENT 'Config update timestamp'
)
USING DELTA
COMMENT 'Metadata configuration for derived columns in Silver Conformance layer';
