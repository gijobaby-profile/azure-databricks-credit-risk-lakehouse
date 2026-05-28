-- =====================================================================
-- File        : 01_metadata/11_update_customer_scd2_dq_rules.sql
-- Purpose     : Disable post-load SCD2 rules from pre-load DQ execution
--               and add safe customer_scd2 verification rules.
-- Notes       : No PRE_LOAD / POST_LOAD column is required.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Disable current SCD2 post-load rules.
--    These rules reference effective_from/effective_to/is_current or
--    target-table state, so they should not run before SCD2 processing.
--    Note: Adding Pre-load and Post-load field in this DQ table is in phase 2
-- ---------------------------------------------------------------------

UPDATE credit_risk_dev.dq.data_quality_rules
SET
    is_active = false,
    updated_timestamp = current_timestamp()
WHERE target_catalog_name = 'credit_risk_dev'
  AND target_schema_name = 'silver'
  AND target_table_name = 'conformed_customer_scd2'
  AND rule_id IN ('DQ_CONF_CUST_001', 'DQ_CONF_CUST_002');


-- ---------------------------------------------------------------------
-- 2. Remove only the new replacement rules if they already exist.
--    This keeps the script rerunnable without creating duplicates.
-- ---------------------------------------------------------------------

DELETE FROM credit_risk_dev.dq.data_quality_rules
WHERE target_catalog_name = 'credit_risk_dev'
  AND target_schema_name = 'silver'
  AND target_table_name = 'conformed_customer_scd2'
  AND rule_id IN (
      'DQ_CONF_CUST_003',
      'DQ_CONF_CUST_004',
      'DQ_CONF_CUST_005',
      'DQ_CONF_CUST_006',
      'DQ_CONF_CUST_007'
  );


-- ---------------------------------------------------------------------
-- 3. Insert safe pre-SCD2 verification rules.
--    These rules use only columns available before apply_scd2_merge().
-- ---------------------------------------------------------------------

INSERT INTO credit_risk_dev.dq.data_quality_rules
(
    rule_id,
    rule_name,
    rule_description,
    rule_type,
    target_catalog_name,
    target_schema_name,
    target_table_name,
    target_column_name,
    rule_sql_expression,
    severity,
    is_active,
    created_timestamp,
    updated_timestamp
)
SELECT
    'DQ_CONF_CUST_003',
    'customer_business_key_not_null',
    'Customer ID, application source, and business date must not be null before SCD2 processing',
    'NOT_NULL',
    'credit_risk_dev',
    'silver',
    'conformed_customer_scd2',
    'customer_id',
    'customer_id IS NULL OR application_source IS NULL OR business_dt IS NULL',
    'CRITICAL',
    true,
    current_timestamp(),
    current_timestamp()

UNION ALL
SELECT
    'DQ_CONF_CUST_004',
    'application_source_valid',
    'Application source must be either train or test',
    'DOMAIN_CHECK',
    'credit_risk_dev',
    'silver',
    'conformed_customer_scd2',
    'application_source',
    'application_source IS NOT NULL AND application_source NOT IN (''train'', ''test'')',
    'HIGH',
    true,
    current_timestamp(),
    current_timestamp()

UNION ALL
SELECT
    'DQ_CONF_CUST_005',
    'customer_record_hash_not_null',
    'Customer record hash must be generated before SCD2 comparison',
    'NOT_NULL',
    'credit_risk_dev',
    'silver',
    'conformed_customer_scd2',
    'record_hash',
    'record_hash IS NULL',
    'CRITICAL',
    true,
    current_timestamp(),
    current_timestamp()

UNION ALL
SELECT
    'DQ_CONF_CUST_006',
    'customer_id_positive',
    'Customer ID must be a positive business identifier',
    'RANGE_CHECK',
    'credit_risk_dev',
    'silver',
    'conformed_customer_scd2',
    'customer_id',
    'customer_id <= 0',
    'HIGH',
    true,
    current_timestamp(),
    current_timestamp()

UNION ALL
SELECT
    'DQ_CONF_CUST_007',
    'customer_numeric_attributes_valid',
    'Customer numeric attributes must not contain invalid negative values',
    'RANGE_CHECK',
    'credit_risk_dev',
    'silver',
    'conformed_customer_scd2',
    'income_total',
    'children_count < 0 OR family_members_count < 0 OR income_total < 0',
    'HIGH',
    true,
    current_timestamp(),
    current_timestamp();


-- ---------------------------------------------------------------------
-- 4. Verification query.
-- ---------------------------------------------------------------------

SELECT
    rule_id,
    rule_name,
    rule_type,
    target_table_name,
    target_column_name,
    severity,
    is_active,
    rule_sql_expression
FROM credit_risk_dev.dq.data_quality_rules
WHERE target_catalog_name = 'credit_risk_dev'
  AND target_schema_name = 'silver'
  AND target_table_name = 'conformed_customer_scd2'
ORDER BY rule_id;
