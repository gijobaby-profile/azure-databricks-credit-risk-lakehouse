-- =====================================================================
-- File        : 01_metadata/09_insert_silver_conformance_derived_column_config.sql
-- Purpose     : Insert metadata-driven derived column expressions
-- =====================================================================

DELETE FROM credit_risk_dev.config.silver_conformance_derived_column_config;

INSERT INTO credit_risk_dev.config.silver_conformance_derived_column_config
SELECT 'loan_application', 'age_years', 'round(abs(days_birth) / 365.25, 2)', 'DECIMAL(10,2)', true, 1, current_timestamp(), current_timestamp()
UNION ALL SELECT 'loan_application', 'employment_years', 'CASE WHEN days_employed IS NULL THEN NULL ELSE round(abs(days_employed) / 365.25, 2) END', 'DECIMAL(10,2)', true, 2, current_timestamp(), current_timestamp()
UNION ALL SELECT 'loan_application', 'credit_income_ratio', 'CASE WHEN income_total IS NULL OR income_total = 0 THEN NULL ELSE round(credit_amount / income_total, 6) END', 'DECIMAL(18,6)', true, 3, current_timestamp(), current_timestamp()
UNION ALL SELECT 'loan_application', 'annuity_income_ratio', 'CASE WHEN income_total IS NULL OR income_total = 0 THEN NULL ELSE round(annuity_amount / income_total, 6) END', 'DECIMAL(18,6)', true, 4, current_timestamp(), current_timestamp()

UNION ALL SELECT 'bureau_credit', 'is_active_credit', 'CASE WHEN lower(credit_active) = ''active'' THEN true ELSE false END', 'BOOLEAN', true, 1, current_timestamp(), current_timestamp()
UNION ALL SELECT 'bureau_credit', 'is_closed_credit', 'CASE WHEN lower(credit_active) = ''closed'' THEN true ELSE false END', 'BOOLEAN', true, 2, current_timestamp(), current_timestamp()
UNION ALL SELECT 'bureau_credit', 'has_overdue', 'CASE WHEN credit_day_overdue > 0 OR credit_overdue_amount > 0 THEN true ELSE false END', 'BOOLEAN', true, 3, current_timestamp(), current_timestamp()
UNION ALL SELECT 'bureau_credit', 'debt_to_credit_ratio', 'CASE WHEN credit_amount IS NULL OR credit_amount = 0 THEN NULL ELSE round(credit_debt_amount / credit_amount, 6) END', 'DECIMAL(18,6)', true, 4, current_timestamp(), current_timestamp()

UNION ALL SELECT 'previous_application', 'is_approved', 'CASE WHEN lower(application_status) = ''approved'' THEN true ELSE false END', 'BOOLEAN', true, 1, current_timestamp(), current_timestamp()
UNION ALL SELECT 'previous_application', 'is_refused', 'CASE WHEN lower(application_status) = ''refused'' THEN true ELSE false END', 'BOOLEAN', true, 2, current_timestamp(), current_timestamp()
UNION ALL SELECT 'previous_application', 'approval_credit_ratio', 'CASE WHEN application_amount IS NULL OR application_amount = 0 THEN NULL ELSE round(credit_amount / application_amount, 6) END', 'DECIMAL(18,6)', true, 3, current_timestamp(), current_timestamp()
UNION ALL SELECT 'previous_application', 'down_payment_ratio', 'CASE WHEN goods_price IS NULL OR goods_price = 0 THEN NULL ELSE round(down_payment_amount / goods_price, 6) END', 'DECIMAL(18,6)', true, 4, current_timestamp(), current_timestamp()

UNION ALL SELECT 'credit_card_balance', 'credit_utilization_ratio', 'CASE WHEN credit_limit_actual IS NULL OR credit_limit_actual = 0 THEN NULL ELSE round(balance_amount / credit_limit_actual, 6) END', 'DECIMAL(18,6)', true, 1, current_timestamp(), current_timestamp()
UNION ALL SELECT 'credit_card_balance', 'has_dpd', 'CASE WHEN days_past_due > 0 THEN true ELSE false END', 'BOOLEAN', true, 2, current_timestamp(), current_timestamp()
UNION ALL SELECT 'credit_card_balance', 'has_default_dpd', 'CASE WHEN days_past_due_def > 0 THEN true ELSE false END', 'BOOLEAN', true, 3, current_timestamp(), current_timestamp()
UNION ALL SELECT 'credit_card_balance', 'is_active_contract', 'CASE WHEN lower(contract_status) = ''active'' THEN true ELSE false END', 'BOOLEAN', true, 4, current_timestamp(), current_timestamp()

UNION ALL SELECT 'installment_payment', 'payment_delay_days', 'days_entry_payment - days_instalment', 'INT', true, 1, current_timestamp(), current_timestamp()
UNION ALL SELECT 'installment_payment', 'payment_amount_diff', 'payment_amount - installment_amount', 'DECIMAL(18,2)', true, 2, current_timestamp(), current_timestamp()
UNION ALL SELECT 'installment_payment', 'is_late_payment', 'CASE WHEN days_entry_payment - days_instalment > 0 THEN true ELSE false END', 'BOOLEAN', true, 3, current_timestamp(), current_timestamp()
UNION ALL SELECT 'installment_payment', 'is_underpayment', 'CASE WHEN payment_amount < installment_amount THEN true ELSE false END', 'BOOLEAN', true, 4, current_timestamp(), current_timestamp()

UNION ALL SELECT 'pos_cash_balance', 'has_dpd', 'CASE WHEN days_past_due > 0 THEN true ELSE false END', 'BOOLEAN', true, 1, current_timestamp(), current_timestamp()
UNION ALL SELECT 'pos_cash_balance', 'has_default_dpd', 'CASE WHEN days_past_due_def > 0 THEN true ELSE false END', 'BOOLEAN', true, 2, current_timestamp(), current_timestamp()
UNION ALL SELECT 'pos_cash_balance', 'is_active_contract', 'CASE WHEN lower(contract_status) = ''active'' THEN true ELSE false END', 'BOOLEAN', true, 3, current_timestamp(), current_timestamp()
UNION ALL SELECT 'pos_cash_balance', 'remaining_installment_ratio', 'CASE WHEN installment_count IS NULL OR installment_count = 0 THEN NULL ELSE round(installment_future_count / installment_count, 6) END', 'DECIMAL(18,6)', true, 4, current_timestamp(), current_timestamp();
