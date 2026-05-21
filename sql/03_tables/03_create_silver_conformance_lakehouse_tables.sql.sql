-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Silver conformance layer
-- MAGIC Business/entity-oriented tables. These tables are built from standardized_* tables after business-level joins and rules.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_loan_application (
  customer_id BIGINT,
  application_source STRING,
  target INT,
  contract_type STRING,
  gender STRING,
  own_car_flag STRING,
  own_realty_flag STRING,
  children_count INT,
  income_amount DECIMAL(18,2),
  credit_amount DECIMAL(18,2),
  annuity_amount DECIMAL(18,2),
  goods_price_amount DECIMAL(18,2),
  income_type STRING,
  education_type STRING,
  family_status STRING,
  housing_type STRING,
  occupation_type STRING,
  organization_type STRING,
  employment_days INT,
  age_years DOUBLE,
  income_credit_ratio DOUBLE,
  annuity_income_ratio DOUBLE,
  external_score_1 DOUBLE,
  external_score_2 DOUBLE,
  external_score_3 DOUBLE,
  application_hour INT,
  application_weekday STRING,
  is_current_record BOOLEAN,
  created_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP,
  pipeline_run_id STRING,
  conformance_timestamp TIMESTAMP,
  conformance_date DATE
)
USING DELTA
PARTITIONED BY (conformance_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_customer_profile (
  customer_id BIGINT,
  gender STRING,
  children_count INT,
  family_members_count INT,
  income_type STRING,
  education_type STRING,
  family_status STRING,
  housing_type STRING,
  occupation_type STRING,
  organization_type STRING,
  age_years DOUBLE,
  employment_years DOUBLE,
  own_car_flag STRING,
  own_realty_flag STRING,
  region_population_relative DOUBLE,
  region_rating_client INT,
  region_rating_client_with_city INT,
  created_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP,
  pipeline_run_id STRING,
  conformance_timestamp TIMESTAMP,
  conformance_date DATE
)
USING DELTA
PARTITIONED BY (conformance_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_bureau_credit (
  customer_id BIGINT,
  bureau_credit_id BIGINT,
  credit_active_status STRING,
  credit_currency STRING,
  days_credit INT,
  credit_day_overdue INT,
  days_credit_enddate INT,
  days_enddate_fact INT,
  credit_max_overdue_amount DECIMAL(18,2),
  credit_prolong_count INT,
  credit_sum_amount DECIMAL(18,2),
  credit_sum_debt_amount DECIMAL(18,2),
  credit_sum_limit_amount DECIMAL(18,2),
  credit_sum_overdue_amount DECIMAL(18,2),
  credit_type STRING,
  days_credit_update INT,
  annuity_amount DECIMAL(18,2),
  is_active_credit BOOLEAN,
  is_closed_credit BOOLEAN,
  created_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP,
  pipeline_run_id STRING,
  conformance_timestamp TIMESTAMP,
  conformance_date DATE
)
USING DELTA
PARTITIONED BY (conformance_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_bureau_balance_monthly (
  bureau_credit_id BIGINT,
  months_balance INT,
  status STRING,
  is_delinquent BOOLEAN,
  is_bad_status BOOLEAN,
  created_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP,
  pipeline_run_id STRING,
  conformance_timestamp TIMESTAMP,
  conformance_date DATE
)
USING DELTA
PARTITIONED BY (conformance_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_previous_application (
  previous_application_id BIGINT,
  customer_id BIGINT,
  contract_type STRING,
  annuity_amount DECIMAL(18,2),
  application_amount DECIMAL(18,2),
  credit_amount DECIMAL(18,2),
  down_payment_amount DECIMAL(18,2),
  goods_price_amount DECIMAL(18,2),
  application_weekday STRING,
  application_hour INT,
  last_application_per_contract_flag STRING,
  last_application_in_day_flag INT,
  micro_cash_flag INT,
  down_payment_rate DOUBLE,
  primary_interest_rate DOUBLE,
  privileged_interest_rate DOUBLE,
  cash_loan_purpose STRING,
  contract_status STRING,
  days_decision INT,
  payment_type STRING,
  reject_reason_code STRING,
  client_type STRING,
  goods_category STRING,
  portfolio_name STRING,
  product_type STRING,
  channel_type STRING,
  sellerplace_area INT,
  seller_industry STRING,
  payment_count INT,
  yield_group STRING,
  product_combination STRING,
  days_first_drawing INT,
  days_first_due INT,
  days_last_due_first_version INT,
  days_last_due INT,
  days_termination INT,
  insured_on_approval_flag INT,
  is_approved BOOLEAN,
  is_refused BOOLEAN,
  created_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP,
  pipeline_run_id STRING,
  conformance_timestamp TIMESTAMP,
  conformance_date DATE
)
USING DELTA
PARTITIONED BY (conformance_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_pos_cash_balance_monthly (
  previous_application_id BIGINT,
  customer_id BIGINT,
  months_balance INT,
  installment_count INT,
  future_installment_count INT,
  contract_status STRING,
  days_past_due INT,
  days_past_due_def INT,
  is_late_payment BOOLEAN,
  created_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP,
  pipeline_run_id STRING,
  conformance_timestamp TIMESTAMP,
  conformance_date DATE
)
USING DELTA
PARTITIONED BY (conformance_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_credit_card_balance_monthly (
  previous_application_id BIGINT,
  customer_id BIGINT,
  months_balance INT,
  balance_amount DECIMAL(18,2),
  credit_limit_actual_amount DECIMAL(18,2),
  drawings_atm_current_amount DECIMAL(18,2),
  drawings_current_amount DECIMAL(18,2),
  drawings_other_current_amount DECIMAL(18,2),
  drawings_pos_current_amount DECIMAL(18,2),
  min_regular_payment_amount DECIMAL(18,2),
  payment_current_amount DECIMAL(18,2),
  payment_total_current_amount DECIMAL(18,2),
  receivable_principal_amount DECIMAL(18,2),
  receivable_amount DECIMAL(18,2),
  total_receivable_amount DECIMAL(18,2),
  drawings_atm_current_count INT,
  drawings_current_count INT,
  drawings_other_current_count INT,
  drawings_pos_current_count INT,
  installment_mature_cumulative_count INT,
  contract_status STRING,
  days_past_due INT,
  days_past_due_def INT,
  credit_card_utilization_ratio DOUBLE,
  created_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP,
  pipeline_run_id STRING,
  conformance_timestamp TIMESTAMP,
  conformance_date DATE
)
USING DELTA
PARTITIONED BY (conformance_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_installment_payment (
  previous_application_id BIGINT,
  customer_id BIGINT,
  installment_version_number DOUBLE,
  installment_number INT,
  days_instalment INT,
  days_entry_payment INT,
  instalment_amount DECIMAL(18,2),
  payment_amount DECIMAL(18,2),
  payment_delay_days INT,
  payment_difference_amount DECIMAL(18,2),
  is_late_payment BOOLEAN,
  created_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP,
  pipeline_run_id STRING,
  conformance_timestamp TIMESTAMP,
  conformance_date DATE
)
USING DELTA
PARTITIONED BY (conformance_date);
