-- Databricks notebook source
DESCRIBE history credit_risk_dev.gold.dim_bureau_credit;

-- COMMAND ----------

describe table extended credit_risk_dev.silver.standardized_application_train

-- COMMAND ----------

describe detail credit_risk_dev.silver.standardized_application_train

-- COMMAND ----------

describe history credit_risk_dev.silver.standardized_application_train

-- COMMAND ----------

VACUUM credit_risk_dev.silver.standardized_application_train RETAIN 7 HOURS DRY RUN;
