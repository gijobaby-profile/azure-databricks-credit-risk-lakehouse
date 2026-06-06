-- Databricks notebook source
select * from credit_risk_dev.bronze.column_description

-- COMMAND ----------

describe table extended credit_risk_dev.silver.standardized_application_train

-- COMMAND ----------

describe detail credit_risk_dev.silver.standardized_application_train

-- COMMAND ----------

describe history credit_risk_dev.silver.standardized_application_train

-- COMMAND ----------

VACUUM credit_risk_dev.silver.standardized_application_train RETAIN 7 HOURS DRY RUN;
