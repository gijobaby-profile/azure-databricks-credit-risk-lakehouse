-- Databricks notebook source
-- =====================================================================
-- File        : 03_insert_dependency_rules.sql
-- Purpose     : Insert dependency rules for Conformance and Gold
-- =====================================================================

-- COMMAND ----------

DELETE FROM credit_risk_dev.orchestration.dependency_rule_metadata;

-- COMMAND ----------

-- =====================================================================
-- SILVER CONFORMANCE DEPENDENCIES
-- =====================================================================

INSERT INTO credit_risk_dev.orchestration.dependency_rule_metadata
SELECT * FROM VALUES
(
    'DEP_CUSTOMER_SCD_001',
    'SILVER_CONFORMANCE',
    'customer_scd',
    'dim_customer_scd',
    'customer',
    'application_train',
    'SILVER_STANDARDIZATION',
    true,
    true,
    current_timestamp(),
    current_timestamp()
),
(
    'DEP_BUREAU_CONF_001',
    'SILVER_CONFORMANCE',
    'generic_entity_conformance',
    'conformed_bureau_credit',
    'bureau',
    'bureau',
    'SILVER_STANDARDIZATION',
    true,
    true,
    current_timestamp(),
    current_timestamp()
),
(
    'DEP_BUREAU_CONF_002',
    'SILVER_CONFORMANCE',
    'generic_entity_conformance',
    'conformed_bureau_credit',
    'bureau',
    'bureau_balance',
    'SILVER_STANDARDIZATION',
    true,
    true,
    current_timestamp(),
    current_timestamp()
),
(
    'DEP_LOAN_CONF_001',
    'SILVER_CONFORMANCE',
    'generic_entity_conformance',
    'conformed_loan_application',
    'loan',
    'previous_application',
    'SILVER_STANDARDIZATION',
    true,
    true,
    current_timestamp(),
    current_timestamp()
),
(
    'DEP_PAYMENT_CONF_001',
    'SILVER_CONFORMANCE',
    'generic_entity_conformance',
    'conformed_installment_payment',
    'payment',
    'installments_payments',
    'SILVER_STANDARDIZATION',
    true,
    true,
    current_timestamp(),
    current_timestamp()
),
(
    'DEP_CARD_CONF_001',
    'SILVER_CONFORMANCE',
    'generic_entity_conformance',
    'conformed_credit_card_balance',
    'card',
    'credit_card_balance',
    'SILVER_STANDARDIZATION',
    false,
    true,
    current_timestamp(),
    current_timestamp()
)
AS t
(
    dependency_rule_id,
    target_layer_name,
    target_process_name,
    target_entity_name,
    dependency_group,
    required_entity_name,
    required_layer_name,
    mandatory_flag,
    active_flag,
    created_timestamp,
    updated_timestamp
);

-- COMMAND ----------

-- =====================================================================
-- GOLD DEPENDENCIES
-- =====================================================================

INSERT INTO credit_risk_dev.orchestration.dependency_rule_metadata
SELECT * FROM VALUES
(
    'DEP_GOLD_001',
    'GOLD',
    'gold_risk_mart',
    'GOLD_RISK_MART',
    'all_mandatory',
    'dim_customer_scd',
    'SILVER_CONFORMANCE',
    true,
    true,
    current_timestamp(),
    current_timestamp()
),
(
    'DEP_GOLD_002',
    'GOLD',
    'gold_risk_mart',
    'GOLD_RISK_MART',
    'all_mandatory',
    'conformed_bureau_credit',
    'SILVER_CONFORMANCE',
    true,
    true,
    current_timestamp(),
    current_timestamp()
),
(
    'DEP_GOLD_003',
    'GOLD',
    'gold_risk_mart',
    'GOLD_RISK_MART',
    'all_mandatory',
    'conformed_loan_application',
    'SILVER_CONFORMANCE',
    true,
    true,
    current_timestamp(),
    current_timestamp()
),
(
    'DEP_GOLD_004',
    'GOLD',
    'gold_risk_mart',
    'GOLD_RISK_MART',
    'all_mandatory',
    'conformed_installment_payment',
    'SILVER_CONFORMANCE',
    true,
    true,
    current_timestamp(),
    current_timestamp()
)
AS t
(
    dependency_rule_id,
    target_layer_name,
    target_process_name,
    target_entity_name,
    dependency_group,
    required_entity_name,
    required_layer_name,
    mandatory_flag,
    active_flag,
    created_timestamp,
    updated_timestamp
);
