-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create data quality rule, result, and rejected record tables

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.dq.data_quality_rules (
    rule_id                STRING      COMMENT 'Unique DQ rule identifier',
    rule_name              STRING      COMMENT 'DQ rule name',
    rule_description       STRING      COMMENT 'Detailed rule description',
    rule_type              STRING      COMMENT 'NOT_NULL, RANGE_CHECK, DOMAIN_CHECK, DUPLICATE_CHECK, FK_CHECK, CUSTOM_SQL',
    target_catalog_name    STRING      COMMENT 'Target catalog name',
    target_schema_name     STRING      COMMENT 'Target schema name',
    target_table_name      STRING      COMMENT 'Target table name',
    target_column_name     STRING      COMMENT 'Target column name; nullable for table-level rules',
    rule_sql_expression    STRING      COMMENT 'SQL expression used to identify failed records',
    severity               STRING      COMMENT 'LOW, MEDIUM, HIGH, CRITICAL',
    is_active              BOOLEAN     COMMENT 'Indicates if the rule is active',
    created_timestamp      TIMESTAMP   COMMENT 'Rule creation timestamp',
    updated_timestamp      TIMESTAMP   COMMENT 'Rule last update timestamp'
)
USING DELTA
COMMENT 'Master table containing reusable data quality rules'
;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.dq.data_quality_results (
    dq_result_id           STRING        COMMENT 'Unique DQ result identifier',
    pipeline_run_id        STRING        COMMENT 'Pipeline run identifier',
    rule_id                STRING        COMMENT 'DQ rule identifier',
    table_name             STRING        COMMENT 'Fully qualified table name checked by the rule',
    rule_name              STRING        COMMENT 'DQ rule name',
    rule_type              STRING        COMMENT 'DQ rule type',
    failed_record_count    BIGINT        COMMENT 'Number of records failed',
    total_record_count     BIGINT        COMMENT 'Total records checked',
    passed_record_count    BIGINT        COMMENT 'Number of records passed',
    failed_percentage      DECIMAL(10,4) COMMENT 'Percentage of failed records',
    status                 STRING        COMMENT 'PASSED, FAILED, WARNING',
    severity               STRING        COMMENT 'LOW, MEDIUM, HIGH, CRITICAL',
    checked_timestamp      TIMESTAMP     COMMENT 'DQ check execution timestamp',
    checked_date           DATE          COMMENT 'DQ check execution date for partition pruning'
)
USING DELTA
PARTITIONED BY (checked_date)
COMMENT 'DQ execution result table for Bronze, Silver, and Gold validations'
;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.dq.rejected_records (
    rejection_id           STRING      COMMENT 'Unique rejected record identifier',
    pipeline_run_id        STRING      COMMENT 'Pipeline run identifier',
    source_table_name      STRING      COMMENT 'Source table name',
    target_table_name      STRING      COMMENT 'Target table name',
    rule_id                STRING      COMMENT 'DQ rule identifier',
    rejection_reason       STRING      COMMENT 'Reason for rejection',
    rejected_record_json   STRING      COMMENT 'Rejected record stored as JSON',
    source_file_name       STRING      COMMENT 'Source file name from Bronze metadata when available',
    source_file_path       STRING      COMMENT 'Source file path from Bronze metadata when available',
    quarantine_path        STRING      COMMENT 'External quarantine path if rejected record is also written as a file',
    created_timestamp      TIMESTAMP   COMMENT 'Rejected record creation timestamp',
    created_date           DATE        COMMENT 'Rejected record creation date for partition pruning'
)
USING DELTA
PARTITIONED BY (created_date)
COMMENT 'Rejected and quarantined records from DQ validation failures'
;
