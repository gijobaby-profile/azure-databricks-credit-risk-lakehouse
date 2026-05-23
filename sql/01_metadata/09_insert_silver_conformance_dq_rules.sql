-- =====================================================================
-- File        : 01_metadata/10_insert_silver_conformance_dq_rules.sql
-- Purpose     : Insert business validation rules for Silver Conformance layer
-- Note        : rule_sql_expression must return TRUE for failed records
-- =====================================================================

DELETE FROM credit_risk_dev.dq.data_quality_rules
WHERE target_catalog_name = 'credit_risk_dev'
  AND target_schema_name = 'silver'
  AND target_table_name LIKE 'conformed_%';

INSERT INTO credit_risk_dev.dq.data_quality_rules
SELECT 'DQ_CONF_CUST_001','one_current_customer_record','Only one current SCD2 record should exist per customer','SCD2_VALIDATION','credit_risk_dev','silver','conformed_customer_scd2','customer_id',
'customer_id IN (SELECT customer_id FROM credit_risk_dev.silver.conformed_customer_scd2 WHERE is_current = true GROUP BY customer_id HAVING COUNT(*) > 1)',
'CRITICAL',true,current_timestamp(),current_timestamp()

UNION ALL SELECT 'DQ_CONF_CUST_002','effective_dates_valid','Customer SCD2 effective_from must be less than effective_to','SCD2_VALIDATION','credit_risk_dev','silver','conformed_customer_scd2','effective_from',
'effective_from >= effective_to',
'CRITICAL',true,current_timestamp(),current_timestamp()

UNION ALL SELECT 'DQ_CONF_APP_001','application_source_valid','Application source must be train or test','DOMAIN_CHECK','credit_risk_dev','silver','conformed_loan_application','application_source',
'application_source NOT IN (''train'',''test'')',
'HIGH',true,current_timestamp(),current_timestamp()

UNION ALL SELECT 'DQ_CONF_APP_002','target_train_valid','Target must be 0 or 1 for train records','DOMAIN_CHECK','credit_risk_dev','silver','conformed_loan_application','target',
'application_source = ''train'' AND target NOT IN (0,1)',
'CRITICAL',true,current_timestamp(),current_timestamp()

UNION ALL SELECT 'DQ_CONF_APP_003','target_test_null','Target must be null for test records','DOMAIN_CHECK','credit_risk_dev','silver','conformed_loan_application','target',
'application_source = ''test'' AND target IS NOT NULL',
'HIGH',true,current_timestamp(),current_timestamp()

UNION ALL SELECT 'DQ_CONF_BUR_001','bureau_credit_key_not_null','Bureau credit ID and customer ID must not be null','NOT_NULL','credit_risk_dev','silver','conformed_bureau_credit','bureau_credit_id',
'bureau_credit_id IS NULL OR customer_id IS NULL',
'CRITICAL',true,current_timestamp(),current_timestamp()

UNION ALL SELECT 'DQ_CONF_PREV_001','previous_application_key_not_null','Previous application ID and customer ID must not be null','NOT_NULL','credit_risk_dev','silver','conformed_previous_application','previous_application_id',
'previous_application_id IS NULL OR customer_id IS NULL',
'CRITICAL',true,current_timestamp(),current_timestamp()

UNION ALL SELECT 'DQ_CONF_CC_001','credit_card_balance_key_not_null','Credit card balance business key must not be null','NOT_NULL','credit_risk_dev','silver','conformed_credit_card_balance','customer_id',
'customer_id IS NULL OR previous_application_id IS NULL OR months_balance IS NULL',
'CRITICAL',true,current_timestamp(),current_timestamp()

UNION ALL SELECT 'DQ_CONF_INS_001','installment_payment_amount_non_negative','Installment payment amounts must not be negative','RANGE_CHECK','credit_risk_dev','silver','conformed_installment_payment','payment_amount',
'payment_amount < 0 OR installment_amount < 0',
'HIGH',true,current_timestamp(),current_timestamp()

UNION ALL SELECT 'DQ_CONF_POS_001','pos_cash_balance_key_not_null','POS cash balance business key must not be null','NOT_NULL','credit_risk_dev','silver','conformed_pos_cash_balance','customer_id',
'customer_id IS NULL OR previous_application_id IS NULL OR months_balance IS NULL',
'CRITICAL',true,current_timestamp(),current_timestamp();
