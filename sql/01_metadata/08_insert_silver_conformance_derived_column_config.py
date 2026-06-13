# =====================================================================
# File        : 01_metadata/08_insert_silver_conformance_derived_column_config.py
# Purpose     : Insert metadata-driven Silver Conformance derived column expressions
# Notes       : Python Row-based insert avoids SQL string quote issues
# =====================================================================

from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, BooleanType, TimestampType
)

catalog_name = "credit_risk_dev"
derived_column_config_table = f"{catalog_name}.config.silver_conformance_derived_column_config"


# =====================================================================
# 1. Delete existing derived column configuration
# =====================================================================
spark.sql(f"""
DELETE FROM {derived_column_config_table}
""")

# =====================================================================
# 2. Insert derived column configuration using Python strings
#    This safely preserves expressions containing quoted values such as
#    'active', 'closed', 'approved', and 'refused'.
# =====================================================================
now = datetime.now()


derived_columns = [
    # -----------------------------------------------------------------
    # Loan application derived columns
    # -----------------------------------------------------------------
    Row(
        entity_name="loan_application",
        derived_column_name="age_years",
        derived_sql_expression="round(abs(days_birth) / 365.25, 2)",
        target_data_type="DECIMAL(10,2)",
        is_active=True,
        column_sequence=1,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="loan_application",
        derived_column_name="employment_years",
        derived_sql_expression="CASE WHEN days_employed IS NULL THEN NULL ELSE round(abs(days_employed) / 365.25, 2) END",
        target_data_type="DECIMAL(10,2)",
        is_active=True,
        column_sequence=2,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="loan_application",
        derived_column_name="credit_income_ratio",
        derived_sql_expression="CASE WHEN income_total IS NULL OR income_total = 0 THEN NULL ELSE round(credit_amount / income_total, 6) END",
        target_data_type="DECIMAL(18,6)",
        is_active=True,
        column_sequence=3,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="loan_application",
        derived_column_name="annuity_income_ratio",
        derived_sql_expression="CASE WHEN income_total IS NULL OR income_total = 0 THEN NULL ELSE round(annuity_amount / income_total, 6) END",
        target_data_type="DECIMAL(18,6)",
        is_active=True,
        column_sequence=4,
        created_timestamp=now,
        updated_timestamp=now
    ),

    # -----------------------------------------------------------------
    # Bureau credit derived columns
    # -----------------------------------------------------------------
    Row(
        entity_name="bureau_credit",
        derived_column_name="is_active_credit",
        derived_sql_expression="CASE WHEN lower(credit_active) = 'active' THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=1,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="bureau_credit",
        derived_column_name="is_closed_credit",
        derived_sql_expression="CASE WHEN lower(credit_active) = 'closed' THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=2,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="bureau_credit",
        derived_column_name="has_overdue",
        derived_sql_expression="CASE WHEN credit_day_overdue > 0 OR credit_overdue_amount > 0 THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=3,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="bureau_credit",
        derived_column_name="debt_to_credit_ratio",
        derived_sql_expression="CASE WHEN credit_amount IS NULL OR credit_amount = 0 THEN NULL ELSE round(credit_debt_amount / credit_amount, 6) END",
        target_data_type="DECIMAL(18,6)",
        is_active=True,
        column_sequence=4,
        created_timestamp=now,
        updated_timestamp=now
    ),

    # -----------------------------------------------------------------
    # Previous application derived columns
    # -----------------------------------------------------------------
    Row(
        entity_name="previous_application",
        derived_column_name="is_approved",
        derived_sql_expression="CASE WHEN lower(application_status) = 'approved' THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=1,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="previous_application",
        derived_column_name="is_refused",
        derived_sql_expression="CASE WHEN lower(application_status) = 'refused' THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=2,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="previous_application",
        derived_column_name="approval_credit_ratio",
        derived_sql_expression="CASE WHEN application_amount IS NULL OR application_amount = 0 THEN NULL ELSE round(credit_amount / application_amount, 6) END",
        target_data_type="DECIMAL(18,6)",
        is_active=True,
        column_sequence=3,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="previous_application",
        derived_column_name="down_payment_ratio",
        derived_sql_expression="CASE WHEN goods_price IS NULL OR goods_price = 0 THEN NULL ELSE round(down_payment_amount / goods_price, 6) END",
        target_data_type="DECIMAL(18,6)",
        is_active=True,
        column_sequence=4,
        created_timestamp=now,
        updated_timestamp=now
    ),

    # -----------------------------------------------------------------
    # Credit card balance derived columns
    # -----------------------------------------------------------------
    Row(
        entity_name="credit_card_balance",
        derived_column_name="credit_utilization_ratio",
        derived_sql_expression="CASE WHEN credit_limit_actual IS NULL OR credit_limit_actual = 0 THEN NULL ELSE round(balance_amount / credit_limit_actual, 6) END",
        target_data_type="DECIMAL(18,6)",
        is_active=True,
        column_sequence=1,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="credit_card_balance",
        derived_column_name="has_dpd",
        derived_sql_expression="CASE WHEN days_past_due > 0 THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=2,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="credit_card_balance",
        derived_column_name="has_default_dpd",
        derived_sql_expression="CASE WHEN days_past_due_def > 0 THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=3,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="credit_card_balance",
        derived_column_name="is_active_contract",
        derived_sql_expression="CASE WHEN lower(contract_status) = 'active' THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=4,
        created_timestamp=now,
        updated_timestamp=now
    ),

    # -----------------------------------------------------------------
    # Installment payment derived columns
    # -----------------------------------------------------------------
    Row(
        entity_name="installment_payment",
        derived_column_name="payment_delay_days",
        derived_sql_expression="days_entry_payment - days_instalment",
        target_data_type="INT",
        is_active=True,
        column_sequence=1,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="installment_payment",
        derived_column_name="payment_amount_diff",
        derived_sql_expression="payment_amount - installment_amount",
        target_data_type="DECIMAL(18,2)",
        is_active=True,
        column_sequence=2,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="installment_payment",
        derived_column_name="is_late_payment",
        derived_sql_expression="CASE WHEN days_entry_payment - days_instalment > 0 THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=3,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="installment_payment",
        derived_column_name="is_underpayment",
        derived_sql_expression="CASE WHEN payment_amount < installment_amount THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=4,
        created_timestamp=now,
        updated_timestamp=now
    ),

    # -----------------------------------------------------------------
    # POS cash balance derived columns
    # -----------------------------------------------------------------
    Row(
        entity_name="pos_cash_balance",
        derived_column_name="has_dpd",
        derived_sql_expression="CASE WHEN days_past_due > 0 THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=1,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="pos_cash_balance",
        derived_column_name="has_default_dpd",
        derived_sql_expression="CASE WHEN days_past_due_def > 0 THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=2,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="pos_cash_balance",
        derived_column_name="is_active_contract",
        derived_sql_expression="CASE WHEN lower(contract_status) = 'active' THEN true ELSE false END",
        target_data_type="BOOLEAN",
        is_active=True,
        column_sequence=3,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="pos_cash_balance",
        derived_column_name="remaining_installment_ratio",
        derived_sql_expression="CASE WHEN installment_count IS NULL OR installment_count = 0 THEN NULL ELSE round(installment_future_count / installment_count, 6) END",
        target_data_type="DECIMAL(18,6)",
        is_active=True,
        column_sequence=4,
        created_timestamp=now,
        updated_timestamp=now
    )
]


# =====================================================================
# 3. Define dataframe schema
# =====================================================================
schema = StructType([
    StructField("entity_name", StringType(), False),
    StructField("derived_column_name", StringType(), False),
    StructField("derived_sql_expression", StringType(), False),
    StructField("target_data_type", StringType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("column_sequence", IntegerType(), False),
    StructField("created_timestamp", TimestampType(), False),
    StructField("updated_timestamp", TimestampType(), False),
])


# =====================================================================
# 4. Construct dataframe
# =====================================================================
derived_columns_df = spark.createDataFrame(derived_columns, schema=schema)


# =====================================================================
# 5. Write into config table
# =====================================================================
(
    derived_columns_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(derived_column_config_table)
)


# =====================================================================
# 6. Verify inserted records
# =====================================================================
display(
    spark.sql(f"""
        SELECT
            entity_name,
            derived_column_name,
            derived_sql_expression,
            target_data_type,
            is_active,
            column_sequence
        FROM {derived_column_config_table}
        ORDER BY entity_name, column_sequence, derived_column_name
    """)
)
