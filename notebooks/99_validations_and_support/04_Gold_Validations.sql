-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Row count by business date across Gold outputs

-- COMMAND ----------

SELECT business_dt, COUNT(*) AS row_count
FROM credit_risk_dev.gold.fact_customer_risk_snapshot
GROUP BY business_dt
ORDER BY business_dt DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check customer risk snapshot duplicate keys

-- COMMAND ----------

SELECT customer_id, business_dt, COUNT(*) AS cnt
FROM credit_risk_dev.gold.fact_customer_risk_snapshot
GROUP BY customer_id, business_dt
HAVING COUNT(*) > 1;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check latest Delta operation history

-- COMMAND ----------

DESCRIBE HISTORY credit_risk_dev.gold.fact_customer_risk_snapshot;
