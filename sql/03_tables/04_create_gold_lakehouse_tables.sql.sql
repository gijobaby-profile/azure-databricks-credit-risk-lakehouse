-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Gold layer
-- MAGIC Analytics-ready marts, feature tables, dimensions, and facts

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.dim_customer (
    customer_dim_key BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id BIGINT,
    application_source STRING,
    gender STRING,
    own_car_flag STRING,
    own_realty_flag STRING,
    children_count INT,
    family_members_count DECIMAL(10,2),
    income_total DECIMAL(18,2),
    income_type STRING,
    education_type STRING,
    family_status STRING,
    housing_type STRING,
    occupation_type STRING,
    organization_type STRING,
    age_years DECIMAL(10,2),
    employment_years DECIMAL(10,2),
    region_population_relative DECIMAL(18,8),
    scd_effective_from TIMESTAMP,
    scd_effective_to TIMESTAMP,
    is_current BOOLEAN,
    business_dt DATE,
    source_system STRING,
    pipeline_run_id STRING,
    gold_created_timestamp TIMESTAMP,
    gold_created_date DATE
) USING DELTA PARTITIONED BY (business_dt)
COMMENT 'Gold customer dimension'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.dim_loan_application (
    loan_application_dim_key BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id BIGINT,
    application_source STRING,
    target INT,
    contract_type STRING,
    income_type STRING,
    education_type STRING,
    family_status STRING,
    housing_type STRING,
    occupation_type STRING,
    credit_amount DECIMAL(18,2),
    annuity_amount DECIMAL(18,2),
    goods_price DECIMAL(18,2),
    income_total DECIMAL(18,2),
    age_years DECIMAL(10,2),
    employment_years DECIMAL(10,2),
    credit_income_ratio DECIMAL(18,6),
    annuity_income_ratio DECIMAL(18,6),
    business_dt DATE,
    source_system STRING,
    pipeline_run_id STRING,
    gold_created_timestamp TIMESTAMP,
    gold_created_date DATE
) USING DELTA PARTITIONED BY (business_dt)
COMMENT 'Gold loan application dimension'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.dim_bureau_credit (
    bureau_credit_dim_key BIGINT GENERATED ALWAYS AS IDENTITY,
    bureau_credit_id BIGINT,
    customer_id BIGINT,
    credit_active STRING,
    credit_currency STRING,
    credit_type STRING,
    is_active_credit BOOLEAN,
    is_closed_credit BOOLEAN,
    has_overdue BOOLEAN,
    business_dt DATE,
    source_system STRING,
    pipeline_run_id STRING,
    gold_created_timestamp TIMESTAMP,
    gold_created_date DATE
) USING DELTA PARTITIONED BY (business_dt)
COMMENT 'Gold bureau credit dimension'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_credit_application (
    customer_dim_key BIGINT,
    loan_application_dim_key BIGINT,
    customer_id BIGINT,
    application_source STRING,
    application_count BIGINT,
    default_flag INT,
    credit_amount DECIMAL(18,2),
    annuity_amount DECIMAL(18,2),
    goods_price DECIMAL(18,2),
    income_total DECIMAL(18,2),
    credit_income_ratio DECIMAL(18,6),
    annuity_income_ratio DECIMAL(18,6),
    high_credit_income_flag BOOLEAN,
    high_annuity_income_flag BOOLEAN,
    business_dt DATE,
    pipeline_run_id STRING,
    gold_created_timestamp TIMESTAMP,
    gold_created_date DATE
) USING DELTA PARTITIONED BY (business_dt)
COMMENT 'Gold fact for loan application credit risk analysis'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_bureau_credit_exposure (
    customer_dim_key BIGINT,
    bureau_credit_dim_key BIGINT,
    bureau_credit_id BIGINT,
    customer_id BIGINT,
    bureau_credit_count BIGINT,
    credit_amount DECIMAL(18,2),
    credit_debt_amount DECIMAL(18,2),
    credit_limit_amount DECIMAL(18,2),
    credit_overdue_amount DECIMAL(18,2),
    annuity_amount DECIMAL(18,2),
    debt_to_credit_ratio DECIMAL(18,6),
    has_overdue BOOLEAN,
    business_dt DATE,
    pipeline_run_id STRING,
    gold_created_timestamp TIMESTAMP,
    gold_created_date DATE
) USING DELTA PARTITIONED BY (business_dt)
COMMENT 'Gold fact for external bureau credit exposure'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_credit_card_balance_monthly (
    customer_dim_key BIGINT,
    customer_id BIGINT,
    previous_application_id BIGINT,
    months_balance INT,
    balance_amount DECIMAL(18,2),
    credit_limit_actual DECIMAL(18,2),
    drawing_amount_current DECIMAL(18,2),
    payment_amount_current DECIMAL(18,2),
    payment_total_current DECIMAL(18,2),
    receivable_total_amount DECIMAL(18,2),
    days_past_due INT,
    days_past_due_def INT,
    credit_utilization_ratio DECIMAL(18,6),
    has_dpd BOOLEAN,
    has_default_dpd BOOLEAN,
    is_active_contract BOOLEAN,
    business_dt DATE,
    pipeline_run_id STRING,
    gold_created_timestamp TIMESTAMP,
    gold_created_date DATE
) USING DELTA PARTITIONED BY (business_dt)
COMMENT 'Gold fact for monthly credit card balances'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_installment_payment (
    customer_dim_key BIGINT,
    customer_id BIGINT,
    previous_application_id BIGINT,
    installment_version INT,
    installment_number INT,
    installment_amount DECIMAL(18,2),
    payment_amount DECIMAL(18,2),
    payment_delay_days INT,
    payment_amount_diff DECIMAL(18,2),
    is_late_payment BOOLEAN,
    is_underpayment BOOLEAN,
    business_dt DATE,
    pipeline_run_id STRING,
    gold_created_timestamp TIMESTAMP,
    gold_created_date DATE
) USING DELTA PARTITIONED BY (business_dt)
COMMENT 'Gold fact for installment repayment behaviour'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_pos_cash_balance_monthly (
    customer_dim_key BIGINT,
    customer_id BIGINT,
    previous_application_id BIGINT,
    months_balance INT,
    installment_future_count INT,
    installment_count INT,
    days_past_due INT,
    days_past_due_def INT,
    has_dpd BOOLEAN,
    has_default_dpd BOOLEAN,
    is_active_contract BOOLEAN,
    remaining_installment_ratio DECIMAL(18,6),
    business_dt DATE,
    pipeline_run_id STRING,
    gold_created_timestamp TIMESTAMP,
    gold_created_date DATE
) USING DELTA PARTITIONED BY (business_dt)
COMMENT 'Gold fact for monthly POS/cash balances'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.gold.fact_customer_risk_snapshot (
    customer_dim_key BIGINT,
    customer_id BIGINT,
    application_source STRING,
    application_count BIGINT,
    default_flag INT,
    total_credit_amount DECIMAL(18,2),
    total_annuity_amount DECIMAL(18,2),
    total_income_amount DECIMAL(18,2),
    avg_credit_income_ratio DECIMAL(18,6),
    avg_annuity_income_ratio DECIMAL(18,6),
    bureau_credit_count BIGINT,
    active_bureau_credit_count BIGINT,
    total_bureau_credit_amount DECIMAL(18,2),
    total_bureau_debt_amount DECIMAL(18,2),
    total_bureau_overdue_amount DECIMAL(18,2),
    credit_card_balance_amount DECIMAL(18,2),
    credit_card_limit_amount DECIMAL(18,2),
    max_credit_card_dpd INT,
    installment_count BIGINT,
    late_installment_count BIGINT,
    underpayment_count BIGINT,
    total_installment_amount DECIMAL(18,2),
    total_payment_amount DECIMAL(18,2),
    pos_contract_count BIGINT,
    max_pos_dpd INT,
    active_pos_contract_count BIGINT,
    risk_segment STRING,
    business_dt DATE,
    pipeline_run_id STRING,
    gold_created_timestamp TIMESTAMP,
    gold_created_date DATE
) USING DELTA PARTITIONED BY (business_dt)
COMMENT 'Power BI friendly customer-level credit risk snapshot'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true');
