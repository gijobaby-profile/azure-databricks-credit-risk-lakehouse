-- Databricks notebook source
select * from credit_risk_dev.silver.conformed_customer_scd2

-- COMMAND ----------

select * from credit_risk_dev.config.silver_conformance_derived_column_config

-- COMMAND ----------

select * from credit_risk_dev.dq.data_quality_results

-- COMMAND ----------

select * from credit_risk_dev.dq.data_quality_rules

-- COMMAND ----------

select * from credit_risk_dev.dq.rejected_records limit 200
