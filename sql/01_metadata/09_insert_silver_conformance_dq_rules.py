from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType,
    BooleanType, TimestampType
)

catalog_name = "credit_risk_dev"
dq_rule_table = f"{catalog_name}.dq.data_quality_rules"

# =====================================================================
# 1. delete the old non-Customer related dq rules
# =====================================================================
spark.sql(f"""
DELETE FROM credit_risk_dev.dq.data_quality_rules
WHERE target_catalog_name = 'credit_risk_dev'
  AND target_schema_name = 'silver'
  AND target_table_name LIKE 'conformed_%'
  AND rule_id not like '%_CUST_%'
""")

# =====================================================================
# 3. Insert safe pre-SCD2 rules using Python strings
#    This preserves 'train' and 'test' correctly inside rule_sql_expression.
# =====================================================================
now = datetime.now()

rules = [
    Row(
        rule_id="DQ_CONF_APP_001",
        rule_name="application_source_valid",
        rule_description="Application source must be train or test",
        rule_type="DOMAIN_CHECK",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_loan_application",
        target_column_name="application_source",
        rule_sql_expression="application_source NOT IN ('train','test')",
        severity="HIGH",
        is_active=True,
        created_timestamp=now,
        updated_timestamp=now
    ),
	Row(
        rule_id="DQ_CONF_APP_002",
        rule_name="target_train_valid",
        rule_description="Target must be 0 or 1 for train records",
        rule_type="DOMAIN_CHECK",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_loan_application",
        target_column_name="target",
        rule_sql_expression="application_source = 'train' AND target NOT IN (0,1)",
        severity="CRITICAL",
        is_active=True,
        created_timestamp=now,
        updated_timestamp=now
    ),
	Row(
        rule_id="DQ_CONF_BUR_001",
        rule_name="bureau_credit_key_not_null",
        rule_description="Bureau credit ID and customer ID must not be null",
        rule_type="NOT_NULL",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_bureau_credit",
        target_column_name="bureau_credit_id",
        rule_sql_expression="bureau_credit_id IS NULL OR customer_id IS NULL",
        severity="CRITICAL",
        is_active=True,
        created_timestamp=now,
        updated_timestamp=now
    ),
	Row(
        rule_id="DQ_CONF_PREV_001",
        rule_name="previous_application_key_not_null",
        rule_description="Previous application ID and customer ID must not be null",
        rule_type="NOT_NULL",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_previous_application",
        target_column_name="previous_application_id",
        rule_sql_expression="previous_application_id IS NULL OR customer_id IS NULL",
        severity="CRITICAL",
        is_active=True,
        created_timestamp=now,
        updated_timestamp=now
    ),
	Row(
        rule_id="DQ_CONF_CC_001",
        rule_name="credit_card_balance_key_not_null",
        rule_description="Credit card balance business key must not be null",
        rule_type="NOT_NULL",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_credit_card_balance",
        target_column_name="customer_id",
        rule_sql_expression="customer_id IS NULL OR previous_application_id IS NULL OR months_balance IS NULL",
        severity="CRITICAL",
        is_active=True,
        created_timestamp=now,
        updated_timestamp=now
    ),
	Row(
        rule_id="DQ_CONF_INS_001",
        rule_name="installment_payment_amount_non_negative",
        rule_description="Installment payment amounts must not be negative",
        rule_type="RANGE_CHECK",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_installment_payment",
        target_column_name="payment_amount",
        rule_sql_expression="payment_amount < 0 OR installment_amount < 0",
        severity="HIGH",
        is_active=True,
        created_timestamp=now,
        updated_timestamp=now
    ),
	Row(
        rule_id="DQ_CONF_POS_001",
        rule_name="pos_cash_balance_key_not_null",
        rule_description="POS cash balance business key must not be null",
        rule_type="NOT_NULL",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_pos_cash_balance",
        target_column_name="customer_id",
        rule_sql_expression="customer_id IS NULL OR previous_application_id IS NULL OR months_balance IS NULL",
        severity="CRITICAL",
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
          AND target_table_name like 'conformed%'
          AND rule_id not like '%_CUST_%'
        ORDER BY rule_id
    """)
)