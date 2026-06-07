-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Analysing rejects from rejected_record_json ( using get_json_object )

-- COMMAND ----------

SELECT
    COUNT(*) AS total_rejected,
    SUM(CASE WHEN get_json_object(rejected_record_json, '$.months_balance') IS NULL THEN 1 ELSE 0 END) AS months_balance_null,
    SUM(CASE WHEN get_json_object(rejected_record_json, '$.status') IS NULL THEN 1 ELSE 0 END) AS status_null,
    SUM(CASE WHEN get_json_object(rejected_record_json, '$.business_dt') IS NULL THEN 1 ELSE 0 END) AS business_dt_null
FROM credit_risk_dev.dq.rejected_records
WHERE target_table_name = 'credit_risk_dev.silver.standardized_bureau_balance';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Fetching DDL for a table

-- COMMAND ----------

SELECT
    ordinal_position,
    column_name,
    full_data_type,
    is_nullable,
    comment
FROM credit_risk_dev.information_schema.columns
WHERE table_schema = 'silver'
  AND table_name = 'standardized_bureau_balance'
ORDER BY ordinal_position;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # finding the latest log file

-- COMMAND ----------

-- MAGIC %python
-- MAGIC log_dir = f"/Volumes/{catalog_name}/files/vol_logs_home_credit_dev/pipeline/silver/{entity_name}/"
-- MAGIC
-- MAGIC files = dbutils.fs.ls(log_dir)
-- MAGIC
-- MAGIC latest_file = sorted(files, key=lambda x: x.modificationTime, reverse=True)[0].path
-- MAGIC
-- MAGIC print(f"Latest log file: {latest_file}")
-- MAGIC print(dbutils.fs.head(latest_file, 10000))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify records loaded for one business date and run

-- COMMAND ----------

SELECT business_dt, pipeline_run_id, source_file_name, COUNT(*) AS record_count
FROM credit_risk_dev.bronze.application_train
--WHERE business_dt = DATE '2026-06-02'
GROUP BY business_dt, pipeline_run_id, source_file_name;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify Delta table versions

-- COMMAND ----------

DESCRIBE HISTORY credit_risk_dev.bronze.application_train;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify table file count and location

-- COMMAND ----------

DESCRIBE DETAIL credit_risk_dev.bronze.application_train;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pipeline run status:

-- COMMAND ----------

SELECT *
FROM credit_risk_dev.audit.pipeline_run_log
--WHERE pipeline_run_id = '<pipeline_run_id>';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Table-level load status:

-- COMMAND ----------

SELECT *
FROM credit_risk_dev.audit.table_load_log
WHERE pipeline_run_id = 'manual_test_20260602_001'
ORDER BY start_timestamp;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Error details:

-- COMMAND ----------

SELECT *
FROM credit_risk_dev.audit.error_log
WHERE pipeline_run_id = 'manual_test_20260602_001';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze record count by run:

-- COMMAND ----------

SELECT
    pipeline_run_id,
    source_file_name,
    source_file_path,
    COUNT(*) AS record_count
FROM credit_risk_dev.bronze.application_train
GROUP BY
    pipeline_run_id,
    source_file_name,
    source_file_path;

