-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create Unity Catalog catalog and schemas
-- MAGIC
-- MAGIC Databricks says managed tables and managed volumes store their data and metadata files in the managed storage location, and when define a MANAGED LOCATION, Unity Catalog tracks it as a storage root and creates internal hashed subdirectories under it.
-- MAGIC

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS credit_risk_dev
MANAGED LOCATION 'abfss://uc-managed@stcrcurateddevuks001.dfs.core.windows.net/credit_risk_dev'
COMMENT 'DEV Unity Catalog for Azure Databricks Credit Risk Lakehouse portfolio project';

-- COMMAND ----------

--- Validation
DESCRIBE CATALOG EXTENDED credit_risk_dev;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create schemas

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS credit_risk_dev.bronze
MANAGED LOCATION 'abfss://uc-managed@stcrcurateddevuks001.dfs.core.windows.net/credit_risk_dev/bronze/'
COMMENT 'Bronze schema containing raw Delta tables ingested from Home Credit source CSV files';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS credit_risk_dev.silver
MANAGED LOCATION 'abfss://uc-managed@stcrcurateddevuks001.dfs.core.windows.net/credit_risk_dev/silver/'
COMMENT 'Silver schema containing cleaned, standardized, validated, and conformed credit risk tables';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS credit_risk_dev.gold
MANAGED LOCATION 'abfss://uc-managed@stcrcurateddevuks001.dfs.core.windows.net/credit_risk_dev/gold/'
COMMENT 'Gold schema containing analytics-ready risk marts, feature store tables, and dashboard outputs';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS credit_risk_dev.config
MANAGED LOCATION 'abfss://uc-managed@stcrcurateddevuks001.dfs.core.windows.net/credit_risk_dev/config/'
COMMENT 'Configuration and metadata-driven pipeline control tables';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS credit_risk_dev.audit
MANAGED LOCATION 'abfss://uc-managed@stcrcurateddevuks001.dfs.core.windows.net/credit_risk_dev/audit/'
COMMENT 'Audit schema containing pipeline run history, table load audit, row counts, status, and operational metrics';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS credit_risk_dev.dq
MANAGED LOCATION 'abfss://uc-managed@stcrcurateddevuks001.dfs.core.windows.net/credit_risk_dev/dq/'
COMMENT 'Data quality schema containing DQ rules, DQ execution results, and validation summaries';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS credit_risk_dev.files
MANAGED LOCATION 'abfss://uc-managed@stcrcurateddevuks001.dfs.core.windows.net/credit_risk_dev/files/'
COMMENT 'Schema for external volumes used by pipelines';

-- COMMAND ----------

----Validation
SHOW SCHEMAS in credit_risk_dev
