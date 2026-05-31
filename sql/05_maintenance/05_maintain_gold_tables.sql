-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Gold layer maintenance for Power BI performance

-- COMMAND ----------

OPTIMIZE credit_risk_dev.gold.dim_customer 
ZORDER BY (customer_id, application_source);

-- COMMAND ----------

OPTIMIZE credit_risk_dev.gold.dim_loan_application 
ZORDER BY (customer_id, application_source);

-- COMMAND ----------

OPTIMIZE credit_risk_dev.gold.dim_bureau_credit 
ZORDER BY (customer_id, bureau_credit_id);

-- COMMAND ----------

OPTIMIZE credit_risk_dev.gold.fact_credit_application 
ZORDER BY (customer_id, application_source);

-- COMMAND ----------

OPTIMIZE credit_risk_dev.gold.fact_bureau_credit_exposure 
ZORDER BY (customer_id, bureau_credit_id);

-- COMMAND ----------

OPTIMIZE credit_risk_dev.gold.fact_credit_card_balance_monthly 
ZORDER BY (customer_id, previous_application_id);

-- COMMAND ----------

OPTIMIZE credit_risk_dev.gold.fact_installment_payment 
ZORDER BY (customer_id, previous_application_id);

-- COMMAND ----------

OPTIMIZE credit_risk_dev.gold.fact_pos_cash_balance_monthly 
ZORDER BY (customer_id, previous_application_id);

-- COMMAND ----------

OPTIMIZE credit_risk_dev.gold.fact_customer_risk_snapshot 
ZORDER BY (customer_id, application_source);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### compute statistics for columns

-- COMMAND ----------

ANALYZE TABLE credit_risk_dev.gold.dim_customer 
COMPUTE STATISTICS FOR COLUMNS business_dt, customer_id, application_source;

-- COMMAND ----------

ANALYZE TABLE credit_risk_dev.gold.dim_loan_application 
COMPUTE STATISTICS FOR COLUMNS business_dt, customer_id, application_source, target;

-- COMMAND ----------

ANALYZE TABLE credit_risk_dev.gold.fact_customer_risk_snapshot 
COMPUTE STATISTICS FOR COLUMNS business_dt, customer_id, application_source, risk_segment;

-- COMMAND ----------

ANALYZE TABLE credit_risk_dev.gold.fact_credit_application 
COMPUTE STATISTICS FOR COLUMNS business_dt, customer_id, application_source, default_flag;
