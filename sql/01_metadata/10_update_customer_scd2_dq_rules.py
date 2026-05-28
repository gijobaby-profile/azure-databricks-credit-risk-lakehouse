from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType,
    BooleanType, TimestampType
)

catalog_name = "credit_risk_dev"
dq_rule_table = f"{catalog_name}.dq.data_quality_rules"

# =====================================================================
# 1. Disable the old SCD2 post-load style rules
# =====================================================================
spark.sql(f"""
UPDATE {dq_rule_table}
SET
    is_active = false,
    updated_timestamp = current_timestamp()
WHERE target_catalog_name = 'credit_risk_dev'
  AND target_schema_name = 'silver'
  AND target_table_name = 'conformed_customer_scd2'
  AND rule_id IN ('DQ_CONF_CUST_001', 'DQ_CONF_CUST_002')
""")

# =====================================================================
# 2. Delete replacement customer_scd2 rules if they already exist
# =====================================================================
spark.sql(f"""
DELETE FROM {dq_rule_table}
WHERE target_catalog_name = 'credit_risk_dev'
  AND target_schema_name = 'silver'
  AND target_table_name = 'conformed_customer_scd2'
  AND rule_id IN (
      'DQ_CONF_CUST_003',
      'DQ_CONF_CUST_004',
      'DQ_CONF_CUST_005',
      'DQ_CONF_CUST_006',
      'DQ_CONF_CUST_007'
  )
""")

# =====================================================================
# 3. Insert safe pre-SCD2 rules using Python strings
#    This preserves 'train' and 'test' correctly inside rule_sql_expression.
# =====================================================================
now = datetime.now()

rules = [
    Row(
        rule_id="DQ_CONF_CUST_003",
        rule_name="customer_business_key_not_null",
        rule_description="Customer ID, application source, and business date must not be null before SCD2 processing",
        rule_type="NOT_NULL",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_customer_scd2",
        target_column_name="customer_id",
        rule_sql_expression="customer_id IS NULL OR application_source IS NULL OR business_dt IS NULL",
        severity="CRITICAL",
        is_active=True,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        rule_id="DQ_CONF_CUST_004",
        rule_name="application_source_valid",
        rule_description="Application source must be either train or test",
        rule_type="DOMAIN_CHECK",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_customer_scd2",
        target_column_name="application_source",
        rule_sql_expression="application_source IS NOT NULL AND application_source NOT IN ('train', 'test')",
        severity="HIGH",
        is_active=True,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        rule_id="DQ_CONF_CUST_005",
        rule_name="customer_record_hash_not_null",
        rule_description="Customer record hash must be generated before SCD2 comparison",
        rule_type="NOT_NULL",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_customer_scd2",
        target_column_name="record_hash",
        rule_sql_expression="record_hash IS NULL",
        severity="CRITICAL",
        is_active=True,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        rule_id="DQ_CONF_CUST_006",
        rule_name="customer_id_positive",
        rule_description="Customer ID must be a positive business identifier",
        rule_type="RANGE_CHECK",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_customer_scd2",
        target_column_name="customer_id",
        rule_sql_expression="customer_id <= 0",
        severity="HIGH",
        is_active=True,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        rule_id="DQ_CONF_CUST_007",
        rule_name="customer_numeric_attributes_valid",
        rule_description="Customer numeric attributes must not contain invalid negative values",
        rule_type="RANGE_CHECK",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_customer_scd2",
        target_column_name="income_total",
        rule_sql_expression="children_count < 0 OR family_members_count < 0 OR income_total < 0",
        severity="HIGH",
        is_active=True,
        created_timestamp=now,
        updated_timestamp=now
    )
]

# =====================================================================
# Create Header record
# =====================================================================

schema = StructType([
    StructField("rule_id", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("rule_description", StringType(), True),
    StructField("rule_type", StringType(), False),
    StructField("target_catalog_name", StringType(), False),
    StructField("target_schema_name", StringType(), False),
    StructField("target_table_name", StringType(), False),
    StructField("target_column_name", StringType(), True),
    StructField("rule_sql_expression", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("created_timestamp", TimestampType(), False),
    StructField("updated_timestamp", TimestampType(), False),
])

# =====================================================================
# Construct dataframe
# =====================================================================
rules_df = spark.createDataFrame(rules, schema=schema)

# =====================================================================
# write itno table
# =====================================================================
(
    rules_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(dq_rule_table)
)

# =====================================================================
# To verify
# =====================================================================
display(
    spark.sql(f"""
        SELECT
            rule_id,
            rule_name,
            is_active,
            rule_sql_expression
        FROM {dq_rule_table}
        WHERE target_catalog_name = 'credit_risk_dev'
          AND target_schema_name = 'silver'
          AND target_table_name = 'conformed_customer_scd2'
        ORDER BY rule_id
    """)
)