-- Databricks notebook source
SELECT *
FROM credit_risk_dev.config.bronze_ingestion_config
ORDER BY load_sequence;

-- COMMAND ----------

SHOW TABLES IN credit_risk_dev.bronze;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED credit_risk_dev.bronze.application_train;

-- COMMAND ----------

select * from credit_risk_dev.bronze.application_train

-- COMMAND ----------

select * from credit_risk_dev.audit.table_load_log

-- COMMAND ----------

describe table  credit_risk_dev.audit.error_log

-- COMMAND ----------

describe table credit_risk_dev.audit.pipeline_run_log

-- COMMAND ----------

select * from credit_risk_dev.audit.table_load_log

-- COMMAND ----------

select * from credit_risk_dev.bronze.application_train

-- COMMAND ----------

SELECT *
FROM credit_risk_dev.audit.pipeline_run_log
ORDER BY created_timestamp DESC
LIMIT 20;
