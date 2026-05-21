-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Optimize Gold Delta tables for query performance

-- COMMAND ----------

OPTIMIZE credit_risk_dev.gold.loan_default_feature_store
ZORDER BY (customer_id);

-- COMMAND ----------

OPTIMIZE credit_risk_dev.gold.customer_credit_risk_profile
ZORDER BY (customer_id);
