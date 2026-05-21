-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Collect table statistics for query optimization

-- COMMAND ----------

ANALYZE TABLE credit_risk_dev.bronze.application_train COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.bronze.application_test COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.bronze.bureau COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.bronze.bureau_balance COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.bronze.previous_application COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.bronze.pos_cash_balance COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.bronze.credit_card_balance COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.bronze.installments_payments COMPUTE STATISTICS;

-- COMMAND ----------

ANALYZE TABLE credit_risk_dev.silver.standardized_application_train COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.silver.standardized_application_test COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.silver.standardized_bureau COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.silver.standardized_bureau_balance COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.silver.standardized_previous_application COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.silver.standardized_pos_cash_balance COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.silver.standardized_credit_card_balance COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.silver.standardized_installments_payments COMPUTE STATISTICS;

-- COMMAND ----------

ANALYZE TABLE credit_risk_dev.silver.conformed_customer COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.silver.conformed_loan_application COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.silver.conformed_bureau_summary COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.silver.conformed_payment_behavior COMPUTE STATISTICS;

-- COMMAND ----------

ANALYZE TABLE credit_risk_dev.gold.customer_risk_summary COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.gold.affordability_risk_mart COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.gold.bureau_credit_risk_summary COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.gold.payment_behavior_mart COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.gold.loan_default_feature_store COMPUTE STATISTICS;
ANALYZE TABLE credit_risk_dev.gold.customer_credit_risk_profile COMPUTE STATISTICS;
