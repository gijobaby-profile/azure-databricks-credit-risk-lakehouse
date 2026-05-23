-- =====================================================================
-- File        : 01_metadata/08_insert_silver_conformance_entity_config.sql
-- Purpose     : Insert metadata for Silver Conformance entities
-- =====================================================================

DELETE FROM credit_risk_dev.config.silver_conformance_entity_config;

INSERT INTO credit_risk_dev.config.silver_conformance_entity_config
SELECT
    'customer_scd2',
    'credit_risk_dev',
    'silver',
    'conformed_customer_scd2',
    '
    SELECT DISTINCT
        customer_id,
        gender,
        own_car_flag,
        own_realty_flag,
        children_count,
        family_members_count,
        income_total,
        income_type,
        education_type,
        family_status,
        housing_type,
        occupation_type,
        organization_type,
        days_birth,
        days_employed,
        region_population_relative,
        source_system,
        pipeline_run_id,
        standardization_timestamp
    FROM (
        SELECT *, ''train'' AS application_source FROM credit_risk_dev.silver.standardized_application_train
        UNION BY NAME
        SELECT *, ''test'' AS application_source FROM credit_risk_dev.silver.standardized_application_test
    )
    WHERE customer_id IS NOT NULL
    ',
    'SCD2',
    array('customer_id'),
    array('gender','own_car_flag','own_realty_flag','children_count','family_members_count','income_total','income_type','education_type','family_status','housing_type','occupation_type','organization_type','days_birth','days_employed','region_population_relative'),
    'standardization_timestamp',
    true,
    true,
    1,
    current_timestamp(),
    current_timestamp()

UNION ALL
SELECT
    'loan_application',
    'credit_risk_dev',
    'silver',
    'conformed_loan_application',
    '
    SELECT
        customer_id,
        application_source,
        target,
        contract_type,
        credit_amount,
        annuity_amount,
        goods_price,
        income_total,
        income_type,
        education_type,
        family_status,
        housing_type,
        occupation_type,
        days_birth,
        days_employed,
        days_registration,
        days_id_publish,
        source_system,
        pipeline_run_id,
        standardization_timestamp
    FROM (
        SELECT *, ''train'' AS application_source FROM credit_risk_dev.silver.standardized_application_train
        UNION BY NAME
        SELECT *, ''test'' AS application_source FROM credit_risk_dev.silver.standardized_application_test
    )
    ',
    'REPLACE_BUSINESS_DT',
    array('business_dt','customer_id','application_source'),
    array('customer_id','application_source','target','contract_type','credit_amount','annuity_amount','goods_price','income_total','income_type','education_type','family_status','housing_type','occupation_type','days_birth','days_employed','days_registration','days_id_publish'),
    'standardization_timestamp',
    false,
    true,
    2,
    current_timestamp(),
    current_timestamp()

UNION ALL
SELECT 'bureau_credit','credit_risk_dev','silver','conformed_bureau_credit',
    '
    SELECT
        bureau_credit_id, customer_id, credit_active, credit_currency, credit_type,
        days_credit, days_credit_enddate, days_enddate_fact, credit_day_overdue,
        credit_amount, credit_debt_amount, credit_limit_amount, credit_overdue_amount,
        annuity_amount, source_system, pipeline_run_id, standardization_timestamp
    FROM credit_risk_dev.silver.standardized_bureau
    ',
    'REPLACE_BUSINESS_DT',
    array('business_dt','bureau_credit_id','customer_id'),
    array('bureau_credit_id','customer_id','credit_active','credit_currency','credit_type','days_credit','days_credit_enddate','days_enddate_fact','credit_day_overdue','credit_amount','credit_debt_amount','credit_limit_amount','credit_overdue_amount','annuity_amount'),
    'standardization_timestamp', false, true, 3, current_timestamp(), current_timestamp()

UNION ALL
SELECT 'previous_application','credit_risk_dev','silver','conformed_previous_application',
    '
    SELECT
        previous_application_id, customer_id, contract_type, annuity_amount,
        application_amount, credit_amount, down_payment_amount, goods_price,
        application_status, payment_type, rejection_reason, client_type, goods_category,
        portfolio_type, product_type, channel_type, seller_industry, yield_group,
        product_combination, decision_days, first_drawing_days, first_due_days,
        last_due_days, termination_days, source_system, pipeline_run_id,
        standardization_timestamp
    FROM credit_risk_dev.silver.standardized_previous_application
    ',
    'REPLACE_BUSINESS_DT',
    array('business_dt','previous_application_id','customer_id'),
    array('previous_application_id','customer_id','contract_type','annuity_amount','application_amount','credit_amount','down_payment_amount','goods_price','application_status','payment_type','rejection_reason','client_type','goods_category','portfolio_type','product_type','channel_type','seller_industry','yield_group','product_combination','decision_days','first_drawing_days','first_due_days','last_due_days','termination_days'),
    'standardization_timestamp', false, true, 4, current_timestamp(), current_timestamp()

UNION ALL
SELECT 'credit_card_balance','credit_risk_dev','silver','conformed_credit_card_balance',
    '
    SELECT
        previous_application_id, customer_id, months_balance, balance_amount,
        credit_limit_actual, drawing_amount_current, drawing_amount_atm_current,
        drawing_amount_pos_current, installment_mature_cumulative, payment_amount_current,
        payment_total_current, receivable_principal_amount, receivable_total_amount,
        days_past_due, days_past_due_def, contract_status, source_system,
        pipeline_run_id, standardization_timestamp
    FROM credit_risk_dev.silver.standardized_credit_card_balance
    ',
    'REPLACE_BUSINESS_DT',
    array('business_dt','customer_id','previous_application_id','months_balance'),
    array('customer_id','previous_application_id','months_balance','balance_amount','credit_limit_actual','drawing_amount_current','drawing_amount_atm_current','drawing_amount_pos_current','payment_amount_current','payment_total_current','receivable_principal_amount','receivable_total_amount','days_past_due','days_past_due_def','contract_status'),
    'standardization_timestamp', false, true, 5, current_timestamp(), current_timestamp()

UNION ALL
SELECT 'installment_payment','credit_risk_dev','silver','conformed_installment_payment',
    '
    SELECT
        previous_application_id, customer_id, installment_version, installment_number,
        days_instalment, days_entry_payment, installment_amount, payment_amount,
        source_system, pipeline_run_id, standardization_timestamp
    FROM credit_risk_dev.silver.standardized_installments_payments
    ',
    'REPLACE_BUSINESS_DT',
    array('business_dt','customer_id','previous_application_id','installment_number','installment_version'),
    array('customer_id','previous_application_id','installment_version','installment_number','days_instalment','days_entry_payment','installment_amount','payment_amount'),
    'standardization_timestamp', false, true, 6, current_timestamp(), current_timestamp()

UNION ALL
SELECT 'pos_cash_balance','credit_risk_dev','silver','conformed_pos_cash_balance',
    '
    SELECT
        previous_application_id, customer_id, months_balance, installment_future_count,
        installment_count, contract_status, days_past_due, days_past_due_def,
        source_system, pipeline_run_id, standardization_timestamp
    FROM credit_risk_dev.silver.standardized_pos_cash_balance
    ',
    'REPLACE_BUSINESS_DT',
    array('business_dt','customer_id','previous_application_id','months_balance'),
    array('customer_id','previous_application_id','months_balance','installment_future_count','installment_count','contract_status','days_past_due','days_past_due_def'),
    'standardization_timestamp', false, true, 7, current_timestamp(), current_timestamp();
