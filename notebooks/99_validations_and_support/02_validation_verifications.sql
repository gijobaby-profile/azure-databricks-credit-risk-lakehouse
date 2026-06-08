-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Column-level mapping and validation policy

-- COMMAND ----------

select * from credit_risk_dev.config.silver_column_config

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Business DQ metadata table.

-- COMMAND ----------

select * from credit_risk_dev.dq.data_quality_rules

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DQ summary table

-- COMMAND ----------

select * from credit_risk_dev.dq.data_quality_results

-- COMMAND ----------

SELECT target_table_name, COUNT(*) AS rejected_count, rule_id, rejection_reason
FROM credit_risk_dev.dq.rejected_records
WHERE pipeline_run_id = 'manual_test_20260602_001'
  --AND target_table_name = 'credit_risk_dev.silver.standardized_bureau_balance'
GROUP BY rule_id, rejection_reason,target_table_name
ORDER BY rejected_count DESC;


-- COMMAND ----------

SELECT rejected_record_json
FROM credit_risk_dev.dq.rejected_records
WHERE pipeline_run_id = 'manual_test_20260602_001'
LIMIT 20;

