-- Databricks notebook source
DELETE FROM credit_risk_dev.dq.data_quality_rules
WHERE rule_id IN (
    'DQ_APP_001',
    'DQ_APP_002',
    'DQ_APP_003',
    'DQ_BUR_001',
    'DQ_INS_001'
);


-- COMMAND ----------

INSERT INTO credit_risk_dev.dq.data_quality_rules
SELECT
    'DQ_APP_001',
    'customer_id_not_null',
    'Customer ID must not be null in loan application table',
    'NOT_NULL',
    'credit_risk_dev',
    'silver',
    'loan_application',
    'customer_id',
    'customer_id IS NULL',
    'CRITICAL',
    true,
    current_timestamp(),
    current_timestamp()

UNION ALL
SELECT
    'DQ_APP_002',
    'target_valid_values',
    'Target must contain only 0 or 1 for train dataset',
    'DOMAIN_CHECK',
    'credit_risk_dev',
    'silver',
    'loan_application',
    'target',
    'target NOT IN (0,1) AND application_source = ''train''',
    'CRITICAL',
    true,
    current_timestamp(),
    current_timestamp()

UNION ALL
SELECT
    'DQ_APP_003',
    'credit_amount_non_negative',
    'Credit amount must not be negative',
    'RANGE_CHECK',
    'credit_risk_dev',
    'silver',
    'loan_application',
    'credit_amount',
    'credit_amount < 0',
    'HIGH',
    true,
    current_timestamp(),
    current_timestamp()

UNION ALL
SELECT
    'DQ_BUR_001',
    'bureau_credit_id_not_null',
    'Bureau credit ID must not be null',
    'NOT_NULL',
    'credit_risk_dev',
    'silver',
    'bureau_credit',
    'bureau_credit_id',
    'bureau_credit_id IS NULL',
    'CRITICAL',
    true,
    current_timestamp(),
    current_timestamp()

UNION ALL
SELECT
    'DQ_INS_001',
    'installment_payment_amount_non_negative',
    'Installment payment amount must not be negative',
    'RANGE_CHECK',
    'credit_risk_dev',
    'silver',
    'installment_payment',
    'payment_amount',
    'payment_amount < 0',
    'HIGH',
    true,
    current_timestamp(),
    current_timestamp();
