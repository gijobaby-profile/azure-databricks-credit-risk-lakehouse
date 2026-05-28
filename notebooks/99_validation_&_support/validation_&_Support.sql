-- Databricks notebook source
select * from credit_risk_dev.dq.data_quality_rules
WHERE target_catalog_name = 'credit_risk_dev'
  AND target_schema_name = 'silver'
  AND target_table_name = 'conformed_customer_scd2'
  --AND rule_id IN ('DQ_CONF_CUST_001', 'DQ_CONF_CUST_002');

-- COMMAND ----------

SELECT customer_id FROM credit_risk_dev.silver.conformed_customer_scd2
 WHERE is_current = true GROUP BY customer_id HAVING COUNT(*) > 1
