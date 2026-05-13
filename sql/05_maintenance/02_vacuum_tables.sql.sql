-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Remove obsolete Delta files after retention period

-- COMMAND ----------

VACUUM credit_risk_dev.bronze.application_train RETAIN 168 HOURS;
VACUUM credit_risk_dev.bronze.application_test RETAIN 168 HOURS;
VACUUM credit_risk_dev.bronze.bureau RETAIN 168 HOURS;
VACUUM credit_risk_dev.bronze.bureau_balance RETAIN 168 HOURS;
VACUUM credit_risk_dev.bronze.previous_application RETAIN 168 HOURS;
VACUUM credit_risk_dev.bronze.pos_cash_balance RETAIN 168 HOURS;
VACUUM credit_risk_dev.bronze.credit_card_balance RETAIN 168 HOURS;
VACUUM credit_risk_dev.bronze.installments_payments RETAIN 168 HOURS;

-- COMMAND ----------

VACUUM credit_risk_dev.silver.standardized_application_train RETAIN 168 HOURS;
VACUUM credit_risk_dev.silver.standardized_application_test RETAIN 168 HOURS;
VACUUM credit_risk_dev.silver.standardized_bureau RETAIN 168 HOURS;
VACUUM credit_risk_dev.silver.standardized_bureau_balance RETAIN 168 HOURS;
VACUUM credit_risk_dev.silver.standardized_previous_application RETAIN 168 HOURS;
VACUUM credit_risk_dev.silver.standardized_pos_cash_balance RETAIN 168 HOURS;
VACUUM credit_risk_dev.silver.standardized_credit_card_balance RETAIN 168 HOURS;
VACUUM credit_risk_dev.silver.standardized_installments_payments RETAIN 168 HOURS;

-- COMMAND ----------

VACUUM credit_risk_dev.silver.conformed_customer RETAIN 168 HOURS;
VACUUM credit_risk_dev.silver.conformed_loan_application RETAIN 168 HOURS;
VACUUM credit_risk_dev.silver.conformed_bureau_summary RETAIN 168 HOURS;
VACUUM credit_risk_dev.silver.conformed_payment_behavior RETAIN 168 HOURS;

-- COMMAND ----------

VACUUM credit_risk_dev.gold.customer_risk_summary RETAIN 168 HOURS;
VACUUM credit_risk_dev.gold.affordability_risk_mart RETAIN 168 HOURS;
VACUUM credit_risk_dev.gold.bureau_credit_risk_summary RETAIN 168 HOURS;
VACUUM credit_risk_dev.gold.payment_behavior_mart RETAIN 168 HOURS;
VACUUM credit_risk_dev.gold.loan_default_feature_store RETAIN 168 HOURS;
VACUUM credit_risk_dev.gold.customer_credit_risk_profile RETAIN 168 HOURS;
