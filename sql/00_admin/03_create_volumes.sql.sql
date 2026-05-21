-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create Unity Catalog external volumes

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS credit_risk_dev.files.vol_landing_home_credit_dev
LOCATION 'abfss://landing@stcrlandingdevuks001.dfs.core.windows.net/home_credit/'
COMMENT 'External volume for Home Credit source CSV files in landing zone';

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS credit_risk_dev.files.vol_bronze_home_credit_dev
LOCATION 'abfss://bronze@stcrcurateddevuks001.dfs.core.windows.net/home_credit/'
COMMENT 'External volume for Home Credit delta files in bronze zone';

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS credit_risk_dev.files.vol_silver_home_credit_dev
LOCATION 'abfss://silver@stcrcurateddevuks001.dfs.core.windows.net/home_credit/'
COMMENT 'External volume for Home Credit delta files in silver zone';

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS credit_risk_dev.files.vol_gold_home_credit_dev
LOCATION 'abfss://gold@stcrgolddevuks001.dfs.core.windows.net/home_credit/'
COMMENT 'External volume for Home Credit delta files in gold zone';

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS credit_risk_dev.files.vol_powerbi_export_dev
LOCATION 'abfss://powerbi@stcrgolddevuks001.dfs.core.windows.net/home_credit/extracts/'
COMMENT 'External volume for Power BI extracts and dashboard-ready exports';

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS credit_risk_dev.files.vol_metadata_config_dev
LOCATION 'abfss://metadata@stcrcurateddevuks001.dfs.core.windows.net/home_credit/config/'
COMMENT 'External volume for pipeline and table configuration files';

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS credit_risk_dev.files.vol_metadata_control_dev
LOCATION 'abfss://metadata@stcrcurateddevuks001.dfs.core.windows.net/home_credit/control/'
COMMENT 'External volume for watermark, batch control, and restart tracking files';

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS credit_risk_dev.files.vol_metadata_audit_dev
LOCATION 'abfss://metadata@stcrcurateddevuks001.dfs.core.windows.net/home_credit/audit/'
COMMENT 'External volume for pipeline run audit, row counts, status, and operational metrics';

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS credit_risk_dev.files.vol_metadata_dq_dev
LOCATION 'abfss://metadata@stcrcurateddevuks001.dfs.core.windows.net/home_credit/dq/'
COMMENT 'External volume for data quality rule results, failed checks, and validation summaries';

-- COMMAND ----------

DROP VOLUME IF EXISTS credit_risk_dev.files.vol_bronze_checkpoint_dev;
DROP VOLUME IF EXISTS credit_risk_dev.files.vol_silver_checkpoint_dev;
DROP VOLUME IF EXISTS credit_risk_dev.files.vol_gold_checkpoint_dev;


-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS credit_risk_dev.files.vol_logs_home_credit_dev
LOCATION 'abfss://logs@stcrcurateddevuks001.dfs.core.windows.net/home_credit/'
COMMENT 'External volume for Home Credit pipeline runtime logs';
