-- Databricks notebook source
select * from credit_risk_dev.config.bronze_ingestion_config

-- COMMAND ----------

describe table extended credit_risk_dev.silver.standardized_application_train

-- COMMAND ----------

describe detail credit_risk_dev.silver.standardized_application_train

-- COMMAND ----------

describe history credit_risk_dev.silver.standardized_application_train

-- COMMAND ----------

VACUUM credit_risk_dev.silver.standardized_application_train RETAIN 7 HOURS DRY RUN;
