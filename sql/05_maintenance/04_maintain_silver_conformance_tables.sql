-- =====================================================================
-- File        : 05_maintenance/04_maintain_silver_conformance_tables.sql
-- Purpose     : Optional OPTIMIZE / ANALYZE for Silver Conformance tables
-- =====================================================================

OPTIMIZE credit_risk_dev.silver.conformed_loan_application ZORDER BY (customer_id);
OPTIMIZE credit_risk_dev.silver.conformed_customer_scd2 ZORDER BY (customer_id);
OPTIMIZE credit_risk_dev.silver.conformed_bureau_credit ZORDER BY (customer_id, bureau_credit_id);
OPTIMIZE credit_risk_dev.silver.conformed_previous_application ZORDER BY (customer_id, previous_application_id);
OPTIMIZE credit_risk_dev.silver.conformed_credit_card_balance ZORDER BY (customer_id, previous_application_id);
OPTIMIZE credit_risk_dev.silver.conformed_installment_payment ZORDER BY (customer_id, previous_application_id);
OPTIMIZE credit_risk_dev.silver.conformed_pos_cash_balance ZORDER BY (customer_id, previous_application_id);

ANALYZE TABLE credit_risk_dev.silver.conformed_loan_application COMPUTE STATISTICS FOR COLUMNS customer_id, application_source, target;
ANALYZE TABLE credit_risk_dev.silver.conformed_customer_scd2 COMPUTE STATISTICS FOR COLUMNS customer_id, is_current;
ANALYZE TABLE credit_risk_dev.silver.conformed_bureau_credit COMPUTE STATISTICS FOR COLUMNS customer_id, bureau_credit_id;
ANALYZE TABLE credit_risk_dev.silver.conformed_previous_application COMPUTE STATISTICS FOR COLUMNS customer_id, previous_application_id;
ANALYZE TABLE credit_risk_dev.silver.conformed_credit_card_balance COMPUTE STATISTICS FOR COLUMNS customer_id, previous_application_id, months_balance;
ANALYZE TABLE credit_risk_dev.silver.conformed_installment_payment COMPUTE STATISTICS FOR COLUMNS customer_id, previous_application_id, installment_number;
ANALYZE TABLE credit_risk_dev.silver.conformed_pos_cash_balance COMPUTE STATISTICS FOR COLUMNS customer_id, previous_application_id, months_balance;
