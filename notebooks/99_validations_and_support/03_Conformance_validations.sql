-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### SCD2 row counts by business date

-- COMMAND ----------

SELECT 'conformed_customer_scd2' AS table_name, business_dt, COUNT(*) AS row_count
FROM credit_risk_dev.silver.conformed_customer_scd2
GROUP BY business_dt;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SCD2 uniqueness check

-- COMMAND ----------

SELECT customer_id, application_source, COUNT(*) AS current_record_count
FROM credit_risk_dev.silver.conformed_customer_scd2
WHERE is_current = true
GROUP BY customer_id, application_source
HAVING COUNT(*) > 1;

