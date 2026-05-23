-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Silver conformance layer
-- MAGIC Business/entity-oriented tables. These tables are built from standardized_* tables after business-level joins and rules.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_customer_scd2 (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for customer version',
    customer_id BIGINT COMMENT 'Business customer identifier',

    gender STRING COMMENT 'Customer gender',
    own_car_flag STRING COMMENT 'Car ownership flag',
    own_realty_flag STRING COMMENT 'Real estate ownership flag',
    children_count INT COMMENT 'Number of children',
    family_members_count DECIMAL(10,2) COMMENT 'Number of family members',

    income_total DECIMAL(18,2) COMMENT 'Total customer income',
    income_type STRING COMMENT 'Customer income type',
    education_type STRING COMMENT 'Customer education type',
    family_status STRING COMMENT 'Customer family status',
    housing_type STRING COMMENT 'Customer housing type',
    occupation_type STRING COMMENT 'Customer occupation type',
    organization_type STRING COMMENT 'Customer organization type',

    days_birth INT COMMENT 'Customer age represented as days from application date',
    days_employed INT COMMENT 'Employment duration represented as days',
    region_population_relative DECIMAL(18,8) COMMENT 'Relative population of customer region',

    record_hash STRING COMMENT 'Hash of tracked SCD2 attributes',
    effective_from TIMESTAMP COMMENT 'Version effective-from timestamp',
    effective_to TIMESTAMP COMMENT 'Version effective-to timestamp',
    is_current BOOLEAN COMMENT 'Current version indicator',

    business_dt DATE COMMENT 'Business/snapshot date represented by this conformance batch',
    source_system STRING COMMENT 'Source system identifier',
    pipeline_run_id STRING COMMENT 'Pipeline run identifier',
    created_timestamp TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp TIMESTAMP COMMENT 'Record update timestamp',
    created_date DATE COMMENT 'Record creation date'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Silver Conformed customer SCD2 entity for historical customer attribute tracking'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_loan_application (
    loan_application_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for loan application',
    customer_id BIGINT COMMENT 'Customer identifier',
    application_source STRING COMMENT 'Source dataset: train or test',

    target INT COMMENT 'Default indicator for train dataset; null for test dataset',
    contract_type STRING COMMENT 'Loan contract type',
    credit_amount DECIMAL(18,2) COMMENT 'Requested credit amount',
    annuity_amount DECIMAL(18,2) COMMENT 'Loan annuity amount',
    goods_price DECIMAL(18,2) COMMENT 'Goods price',
    income_total DECIMAL(18,2) COMMENT 'Customer income at application time',

    income_type STRING COMMENT 'Income type at application time',
    education_type STRING COMMENT 'Education type at application time',
    family_status STRING COMMENT 'Family status at application time',
    housing_type STRING COMMENT 'Housing type at application time',
    occupation_type STRING COMMENT 'Occupation type at application time',

    days_birth INT COMMENT 'Customer age in relative days',
    days_employed INT COMMENT 'Employment duration in relative days',
    days_registration DECIMAL(18,2) COMMENT 'Registration change in relative days',
    days_id_publish INT COMMENT 'ID publication in relative days',

    age_years DECIMAL(10,2) COMMENT 'Derived customer age in years',
    employment_years DECIMAL(10,2) COMMENT 'Derived employment duration in years',
    credit_income_ratio DECIMAL(18,6) COMMENT 'Credit amount divided by income total',
    annuity_income_ratio DECIMAL(18,6) COMMENT 'Annuity amount divided by income total',

    record_hash STRING COMMENT 'Hash of conformed application business attributes',
    business_dt DATE COMMENT 'Business/snapshot date represented by this conformance batch',
    source_system STRING COMMENT 'Source system identifier',
    pipeline_run_id STRING COMMENT 'Pipeline run identifier',
    created_timestamp TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp TIMESTAMP COMMENT 'Record update timestamp',
    created_date DATE COMMENT 'Record creation date'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Silver Conformed loan application entity combining train and test standardized application data'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_bureau_credit (
    bureau_credit_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for bureau credit',
    bureau_credit_id BIGINT COMMENT 'Natural bureau credit identifier',
    customer_id BIGINT COMMENT 'Customer identifier',

    credit_active STRING COMMENT 'Bureau credit status',
    credit_currency STRING COMMENT 'Bureau credit currency',
    credit_type STRING COMMENT 'Bureau credit type',
    days_credit INT COMMENT 'Credit grant date in relative days',
    days_credit_enddate INT COMMENT 'Expected credit end date in relative days',
    days_enddate_fact INT COMMENT 'Actual credit end date in relative days',
    credit_day_overdue INT COMMENT 'Number of overdue days',

    credit_amount DECIMAL(18,2) COMMENT 'Credit amount',
    credit_debt_amount DECIMAL(18,2) COMMENT 'Current debt amount',
    credit_limit_amount DECIMAL(18,2) COMMENT 'Credit limit amount',
    credit_overdue_amount DECIMAL(18,2) COMMENT 'Overdue amount',
    annuity_amount DECIMAL(18,2) COMMENT 'Bureau credit annuity amount',

    is_active_credit BOOLEAN COMMENT 'Derived active credit indicator',
    is_closed_credit BOOLEAN COMMENT 'Derived closed credit indicator',
    has_overdue BOOLEAN COMMENT 'Derived overdue indicator',
    debt_to_credit_ratio DECIMAL(18,6) COMMENT 'Debt amount divided by credit amount',

    record_hash STRING COMMENT 'Hash of bureau credit business attributes',
    business_dt DATE COMMENT 'Business/snapshot date represented by this conformance batch',
    source_system STRING COMMENT 'Source system identifier',
    pipeline_run_id STRING COMMENT 'Pipeline run identifier',
    created_timestamp TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp TIMESTAMP COMMENT 'Record update timestamp',
    created_date DATE COMMENT 'Record creation date'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Silver Conformed external bureau credit entity for customer credit history analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_previous_application (
    previous_application_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for previous application',
    previous_application_id BIGINT COMMENT 'Previous application identifier',
    customer_id BIGINT COMMENT 'Customer identifier',

    contract_type STRING COMMENT 'Previous application contract type',
    annuity_amount DECIMAL(18,2) COMMENT 'Previous application annuity amount',
    application_amount DECIMAL(18,2) COMMENT 'Previous application requested amount',
    credit_amount DECIMAL(18,2) COMMENT 'Previous application approved credit amount',
    down_payment_amount DECIMAL(18,2) COMMENT 'Down payment amount',
    goods_price DECIMAL(18,2) COMMENT 'Goods price',

    application_status STRING COMMENT 'Previous application status',
    payment_type STRING COMMENT 'Payment type',
    rejection_reason STRING COMMENT 'Rejection reason',
    client_type STRING COMMENT 'Client type',
    goods_category STRING COMMENT 'Goods category',
    portfolio_type STRING COMMENT 'Portfolio type',
    product_type STRING COMMENT 'Product type',
    channel_type STRING COMMENT 'Channel type',
    seller_industry STRING COMMENT 'Seller industry',
    yield_group STRING COMMENT 'Yield group',
    product_combination STRING COMMENT 'Product combination',

    decision_days INT COMMENT 'Decision date in relative days',
    first_drawing_days INT COMMENT 'First drawing date in relative days',
    first_due_days INT COMMENT 'First due date in relative days',
    last_due_days INT COMMENT 'Last due date in relative days',
    termination_days INT COMMENT 'Termination date in relative days',

    is_approved BOOLEAN COMMENT 'Derived approved indicator',
    is_refused BOOLEAN COMMENT 'Derived refused indicator',
    approval_credit_ratio DECIMAL(18,6) COMMENT 'Credit amount divided by application amount',
    down_payment_ratio DECIMAL(18,6) COMMENT 'Down payment divided by goods price',

    record_hash STRING COMMENT 'Hash of previous application business attributes',
    business_dt DATE COMMENT 'Business/snapshot date represented by this conformance batch',
    source_system STRING COMMENT 'Source system identifier',
    pipeline_run_id STRING COMMENT 'Pipeline run identifier',
    created_timestamp TIMESTAMP COMMENT 'Record creation timestamp',
    updated_timestamp TIMESTAMP COMMENT 'Record update timestamp',
    created_date DATE COMMENT 'Record creation date'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Silver Conformed previous application entity for historical credit application behavior analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_credit_card_balance (
    credit_card_balance_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for credit card balance',
    previous_application_id BIGINT COMMENT 'Previous application identifier',
    customer_id BIGINT COMMENT 'Customer identifier',
    months_balance INT COMMENT 'Month balance relative to current application',

    balance_amount DECIMAL(18,2) COMMENT 'Credit card balance amount',
    credit_limit_actual DECIMAL(18,2) COMMENT 'Actual credit card limit',
    drawing_amount_current DECIMAL(18,2) COMMENT 'Current drawing amount',
    drawing_amount_atm_current DECIMAL(18,2) COMMENT 'Current ATM drawing amount',
    drawing_amount_pos_current DECIMAL(18,2) COMMENT 'Current POS drawing amount',
    installment_mature_cumulative DECIMAL(18,2) COMMENT 'Cumulative matured installments',
    payment_amount_current DECIMAL(18,2) COMMENT 'Current payment amount',
    payment_total_current DECIMAL(18,2) COMMENT 'Total current payment amount',
    receivable_principal_amount DECIMAL(18,2) COMMENT 'Receivable principal amount',
    receivable_total_amount DECIMAL(18,2) COMMENT 'Total receivable amount',

    days_past_due INT COMMENT 'Days past due',
    days_past_due_def INT COMMENT 'Days past due with tolerance definition',
    contract_status STRING COMMENT 'Contract status',

    credit_utilization_ratio DECIMAL(18,6) COMMENT 'Balance amount divided by credit limit actual',
    has_dpd BOOLEAN COMMENT 'Days past due indicator',
    has_default_dpd BOOLEAN COMMENT 'Default DPD indicator',
    is_active_contract BOOLEAN COMMENT 'Active contract indicator',

    business_dt DATE COMMENT 'Business/snapshot date represented by this conformance batch',
    source_system STRING COMMENT 'Source system identifier',
    pipeline_run_id STRING COMMENT 'Pipeline run identifier',
    created_timestamp TIMESTAMP COMMENT 'Record creation timestamp',
    created_date DATE COMMENT 'Record creation date'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Silver Conformed credit card balance entity for utilization and delinquency analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_installment_payment (
    installment_payment_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for installment payment',
    previous_application_id BIGINT COMMENT 'Previous application identifier',
    customer_id BIGINT COMMENT 'Customer identifier',

    installment_version INT COMMENT 'Installment version number',
    installment_number INT COMMENT 'Installment sequence number',
    days_instalment INT COMMENT 'Scheduled installment date in relative days',
    days_entry_payment INT COMMENT 'Actual payment date in relative days',
    installment_amount DECIMAL(18,2) COMMENT 'Scheduled installment amount',
    payment_amount DECIMAL(18,2) COMMENT 'Actual payment amount',

    payment_delay_days INT COMMENT 'Actual payment date minus scheduled installment date',
    payment_amount_diff DECIMAL(18,2) COMMENT 'Payment amount minus installment amount',
    is_late_payment BOOLEAN COMMENT 'Late payment indicator',
    is_underpayment BOOLEAN COMMENT 'Underpayment indicator',

    business_dt DATE COMMENT 'Business/snapshot date represented by this conformance batch',
    source_system STRING COMMENT 'Source system identifier',
    pipeline_run_id STRING COMMENT 'Pipeline run identifier',
    created_timestamp TIMESTAMP COMMENT 'Record creation timestamp',
    created_date DATE COMMENT 'Record creation date'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Silver Conformed installment payment entity for repayment behavior analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.silver.conformed_pos_cash_balance (
    pos_cash_balance_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for POS cash balance',
    previous_application_id BIGINT COMMENT 'Previous application identifier',
    customer_id BIGINT COMMENT 'Customer identifier',
    months_balance INT COMMENT 'Month balance relative to current application',

    installment_future_count INT COMMENT 'Number of future installments',
    installment_count INT COMMENT 'Total number of installments',
    contract_status STRING COMMENT 'POS cash contract status',
    days_past_due INT COMMENT 'Days past due',
    days_past_due_def INT COMMENT 'Days past due with tolerance definition',

    has_dpd BOOLEAN COMMENT 'Days past due indicator',
    has_default_dpd BOOLEAN COMMENT 'Default DPD indicator',
    is_active_contract BOOLEAN COMMENT 'Active contract indicator',
    remaining_installment_ratio DECIMAL(18,6) COMMENT 'Future installments divided by total installments',

    business_dt DATE COMMENT 'Business/snapshot date represented by this conformance batch',
    source_system STRING COMMENT 'Source system identifier',
    pipeline_run_id STRING COMMENT 'Pipeline run identifier',
    created_timestamp TIMESTAMP COMMENT 'Record creation timestamp',
    created_date DATE COMMENT 'Record creation date'
)
USING DELTA
PARTITIONED BY (business_dt)
COMMENT 'Silver Conformed POS/cash balance entity for loan lifecycle and delinquency behavior analysis'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

