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

log_dir = f"/Volumes/{catalog_name}/files/vol_logs_home_credit_dev/pipeline/silver/{entity_name}/"

files = dbutils.fs.ls(log_dir)

latest_file = sorted(files, key=lambda x: x.modificationTime, reverse=True)[0].path

print(f"Latest log file: {latest_file}")
print(dbutils.fs.head(latest_file, 10000))
