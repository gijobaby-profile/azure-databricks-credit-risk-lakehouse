-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Gold layer
-- MAGIC Analytics-ready marts, feature tables, dimensions, and facts

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.customer_credit_risk_profile (
  customer_id BIGINT,
  total_bureau_credits INT,
  active_bureau_credits INT,
  closed_bureau_credits INT,
  max_credit_overdue DECIMAL(18,2),
  avg_credit_duration DOUBLE,
  previous_application_count INT,
  approved_previous_application_count INT,
  refused_previous_application_count INT,
  avg_installment_delay_days DOUBLE,
  max_installment_delay_days INT,
  total_late_payments INT,
  credit_card_utilization_ratio DOUBLE,
  risk_band STRING,
  profile_created_timestamp TIMESTAMP,
  pipeline_run_id STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.loan_default_feature_store (
  customer_id BIGINT,
  target INT,
  application_amount DECIMAL(18,2),
  credit_amount DECIMAL(18,2),
  annuity_amount DECIMAL(18,2),
  income_amount DECIMAL(18,2),
  contract_type STRING,
  education_type STRING,
  family_status STRING,
  housing_type STRING,
  employment_days INT,
  age_years DOUBLE,
  income_credit_ratio DOUBLE,
  annuity_income_ratio DOUBLE,
  total_previous_bureau_credits INT,
  active_bureau_credit_count INT,
  closed_bureau_credit_count INT,
  total_previous_credit_amount DECIMAL(18,2),
  avg_previous_credit_amount DECIMAL(18,2),
  max_days_credit_overdue INT,
  total_previous_home_credit_applications INT,
  approved_previous_application_count INT,
  refused_previous_application_count INT,
  avg_installment_delay_days DOUBLE,
  max_installment_delay_days INT,
  total_paid_amount DECIMAL(18,2),
  credit_card_utilization_ratio DOUBLE,
  pos_cash_late_payment_count INT,
  bureau_months_observed INT,
  feature_created_timestamp TIMESTAMP,
  feature_version STRING,
  pipeline_run_id STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.credit_behavior_summary (
  customer_id BIGINT,
  bureau_credit_count INT,
  active_credit_count INT,
  closed_credit_count INT,
  total_credit_amount DECIMAL(18,2),
  total_credit_debt_amount DECIMAL(18,2),
  total_credit_overdue_amount DECIMAL(18,2),
  max_days_credit_overdue INT,
  credit_types_count INT,
  bureau_months_observed INT,
  bad_status_month_count INT,
  summary_created_timestamp TIMESTAMP,
  pipeline_run_id STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.repayment_performance_summary (
  customer_id BIGINT,
  previous_application_count INT,
  installment_count INT,
  total_instalment_amount DECIMAL(18,2),
  total_paid_amount DECIMAL(18,2),
  avg_payment_delay_days DOUBLE,
  max_payment_delay_days INT,
  late_payment_count INT,
  payment_shortfall_amount DECIMAL(18,2),
  pos_cash_late_payment_count INT,
  summary_created_timestamp TIMESTAMP,
  pipeline_run_id STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.application_risk_dashboard (
  customer_id BIGINT,
  target INT,
  contract_type STRING,
  education_type STRING,
  family_status STRING,
  housing_type STRING,
  income_amount DECIMAL(18,2),
  credit_amount DECIMAL(18,2),
  annuity_amount DECIMAL(18,2),
  income_credit_ratio DOUBLE,
  annuity_income_ratio DOUBLE,
  risk_band STRING,
  active_bureau_credit_count INT,
  refused_previous_application_count INT,
  avg_installment_delay_days DOUBLE,
  credit_card_utilization_ratio DOUBLE,
  dashboard_created_timestamp TIMESTAMP,
  pipeline_run_id STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.dim_customer (
  customer_key BIGINT,
  customer_id BIGINT,
  gender STRING,
  age_years DOUBLE,
  income_type STRING,
  education_type STRING,
  family_status STRING,
  housing_type STRING,
  occupation_type STRING,
  organization_type STRING,
  own_car_flag STRING,
  own_realty_flag STRING,
  effective_start_date DATE,
  effective_end_date DATE,
  is_current BOOLEAN
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.dim_contract (
  contract_key BIGINT,
  contract_type STRING,
  payment_type STRING,
  contract_status STRING,
  yield_group STRING,
  product_combination STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.dim_credit_status (
  credit_status_key BIGINT,
  credit_active_status STRING,
  bureau_balance_status STRING,
  is_active_credit BOOLEAN,
  is_delinquent BOOLEAN,
  is_bad_status BOOLEAN
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.dim_product (
  product_key BIGINT,
  product_type STRING,
  portfolio_name STRING,
  goods_category STRING,
  cash_loan_purpose STRING,
  seller_industry STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.dim_calendar (
  calendar_key INT,
  calendar_date DATE,
  year_number INT,
  quarter_number INT,
  month_number INT,
  month_name STRING,
  day_of_month INT,
  day_of_week INT,
  weekday_name STRING,
  is_weekend BOOLEAN
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.dim_channel (
  channel_key BIGINT,
  channel_type STRING,
  sellerplace_area INT,
  organization_type STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_loan_application (
  customer_id BIGINT,
  customer_key BIGINT,
  contract_key BIGINT,
  calendar_key INT,
  target INT,
  income_amount DECIMAL(18,2),
  credit_amount DECIMAL(18,2),
  annuity_amount DECIMAL(18,2),
  goods_price_amount DECIMAL(18,2),
  income_credit_ratio DOUBLE,
  annuity_income_ratio DOUBLE,
  application_count INT,
  pipeline_run_id STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_bureau_credit (
  customer_id BIGINT,
  bureau_credit_id BIGINT,
  customer_key BIGINT,
  credit_status_key BIGINT,
  credit_type STRING,
  credit_sum_amount DECIMAL(18,2),
  credit_sum_debt_amount DECIMAL(18,2),
  credit_sum_limit_amount DECIMAL(18,2),
  credit_sum_overdue_amount DECIMAL(18,2),
  credit_day_overdue INT,
  days_credit INT,
  pipeline_run_id STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_bureau_monthly_balance (
  bureau_credit_id BIGINT,
  months_balance INT,
  credit_status_key BIGINT,
  status STRING,
  is_delinquent BOOLEAN,
  is_bad_status BOOLEAN,
  pipeline_run_id STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_previous_application (
  previous_application_id BIGINT,
  customer_id BIGINT,
  customer_key BIGINT,
  contract_key BIGINT,
  product_key BIGINT,
  channel_key BIGINT,
  application_amount DECIMAL(18,2),
  credit_amount DECIMAL(18,2),
  annuity_amount DECIMAL(18,2),
  down_payment_amount DECIMAL(18,2),
  is_approved BOOLEAN,
  is_refused BOOLEAN,
  days_decision INT,
  pipeline_run_id STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_pos_cash_monthly_balance (
  previous_application_id BIGINT,
  customer_id BIGINT,
  months_balance INT,
  installment_count INT,
  future_installment_count INT,
  days_past_due INT,
  days_past_due_def INT,
  is_late_payment BOOLEAN,
  pipeline_run_id STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_credit_card_monthly_balance (
  previous_application_id BIGINT,
  customer_id BIGINT,
  months_balance INT,
  balance_amount DECIMAL(18,2),
  credit_limit_actual_amount DECIMAL(18,2),
  payment_total_current_amount DECIMAL(18,2),
  total_receivable_amount DECIMAL(18,2),
  drawings_current_count INT,
  credit_card_utilization_ratio DOUBLE,
  days_past_due INT,
  pipeline_run_id STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_installment_payment (
  previous_application_id BIGINT,
  customer_id BIGINT,
  installment_number INT,
  days_instalment INT,
  days_entry_payment INT,
  instalment_amount DECIMAL(18,2),
  payment_amount DECIMAL(18,2),
  payment_delay_days INT,
  is_late_payment BOOLEAN,
  pipeline_run_id STRING
)
USING DELTA;
