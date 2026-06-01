-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build fact_pos_cash_balance_monthly

-- COMMAND ----------

CREATE WIDGET TEXT business_dt DEFAULT "";

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW gold_runtime_parameters AS
SELECT coalesce(to_date(nullif('${business_dt}', '')), current_date()) AS business_dt;

-- COMMAND ----------

DELETE FROM credit_risk_dev.gold.fact_pos_cash_balance_monthly
WHERE business_dt = (SELECT business_dt FROM gold_runtime_parameters);

-- COMMAND ----------


