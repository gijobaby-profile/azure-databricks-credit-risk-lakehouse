# =====================================================================
# File        : 01_metadata/07_insert_silver_conformance_entity_config.py
# Project     : Azure Databricks Credit Risk Lakehouse
# Purpose     : Insert Silver Conformance entity metadata using Python
# Why Python  : Avoid SQL quote escaping issues for source_query values
# =====================================================================

# COMMAND ----------

from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType,
    BooleanType, IntegerType, TimestampType
)

catalog_name = "credit_risk_dev"
config_table = f"{catalog_name}.config.silver_conformance_entity_config"

# COMMAND ----------

customer_scd2_query = """
SELECT DISTINCT
    customer_id,
    application_source,
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
    source_file_name,
    source_file_path,
    pipeline_run_id,
    standardization_timestamp
FROM (
    SELECT
        sk_id_curr AS customer_id,
        'train' AS application_source,
        code_gender AS gender,
        flag_own_car AS own_car_flag,
        flag_own_realty AS own_realty_flag,
        cnt_children AS children_count,
        cnt_fam_members AS family_members_count,
        amt_income_total AS income_total,
        name_income_type AS income_type,
        name_education_type AS education_type,
        name_family_status AS family_status,
        name_housing_type AS housing_type,
        occupation_type,
        organization_type,
        days_birth,
        days_employed,
        CAST(region_population_relative AS DECIMAL(18,8)) AS region_population_relative,
        source_file_name,
        source_file_path,
        pipeline_run_id,
        standardization_timestamp
    FROM credit_risk_dev.silver.standardized_application_train

    UNION ALL

    SELECT
        sk_id_curr AS customer_id,
        'test' AS application_source,
        code_gender AS gender,
        flag_own_car AS own_car_flag,
        flag_own_realty AS own_realty_flag,
        cnt_children AS children_count,
        cnt_fam_members AS family_members_count,
        amt_income_total AS income_total,
        name_income_type AS income_type,
        name_education_type AS education_type,
        name_family_status AS family_status,
        name_housing_type AS housing_type,
        occupation_type,
        organization_type,
        days_birth,
        days_employed,
        CAST(region_population_relative AS DECIMAL(18,8)) AS region_population_relative,
        source_file_name,
        source_file_path,
        pipeline_run_id,
        standardization_timestamp
    FROM credit_risk_dev.silver.standardized_application_test
)
WHERE customer_id IS NOT NULL
"""

loan_application_query = """
SELECT
    sk_id_curr AS customer_id,
    application_source,
    CAST(target AS INT) AS target,
    name_contract_type AS contract_type,
    amt_credit AS credit_amount,
    amt_annuity AS annuity_amount,
    amt_goods_price AS goods_price,
    amt_income_total AS income_total,
    name_income_type AS income_type,
    name_education_type AS education_type,
    name_family_status AS family_status,
    name_housing_type AS housing_type,
    occupation_type,
    days_birth,
    days_employed,
    CAST(days_registration AS DECIMAL(18,2)) AS days_registration,
    days_id_publish,
    source_file_name,
    source_file_path,
    pipeline_run_id,
    standardization_timestamp
FROM (
    SELECT
        sk_id_curr,
		cast(target as int) as target ,
		name_contract_type,
		amt_credit,
		amt_annuity,
		amt_goods_price,
		amt_income_total,
		name_income_type,
		name_education_type,
		name_family_status,
		name_housing_type,
		occupation_type,
		days_birth,
		days_employed,
		days_registration,
		days_id_publish,
		source_file_name,
		source_file_path,
		pipeline_run_id,
		standardization_timestamp,
        'train' AS application_source
    FROM credit_risk_dev.silver.standardized_application_train

    UNION ALL

    SELECT
        sk_id_curr,
		cast(NULL as int) as target,
		name_contract_type,
		amt_credit,
		amt_annuity,
		amt_goods_price,
		amt_income_total,
		name_income_type,
		name_education_type,
		name_family_status,
		name_housing_type,
		occupation_type,
		days_birth,
		days_employed,
		days_registration,
		days_id_publish,
		source_file_name,
		source_file_path,
		pipeline_run_id,
		standardization_timestamp,
        'test' AS application_source
    FROM credit_risk_dev.silver.standardized_application_test
)
"""

bureau_credit_query = """
SELECT
    sk_bureau_id AS bureau_credit_id,
    sk_id_curr AS customer_id,
    credit_active,
    credit_currency,
    credit_type,
    days_credit,
    days_credit_enddate,
    days_enddate_fact,
    credit_day_overdue,
    amt_credit_sum AS credit_amount,
    amt_credit_sum_debt AS credit_debt_amount,
    amt_credit_sum_limit AS credit_limit_amount,
    amt_credit_sum_overdue AS credit_overdue_amount,
    amt_annuity AS annuity_amount,
    source_file_name,
    source_file_path,
    pipeline_run_id,
    standardization_timestamp
FROM credit_risk_dev.silver.standardized_bureau
"""

previous_application_query = """
SELECT
    sk_id_prev AS previous_application_id,
    sk_id_curr AS customer_id,
    name_contract_type AS contract_type,
    amt_annuity AS annuity_amount,
    amt_application AS application_amount,
    amt_credit AS credit_amount,
    amt_down_payment AS down_payment_amount,
    amt_goods_price AS goods_price,
    name_contract_status AS application_status,
    name_payment_type AS payment_type,
    code_reject_reason AS rejection_reason,
    name_client_type AS client_type,
    name_goods_category AS goods_category,
    name_portfolio AS portfolio_type,
    name_product_type AS product_type,
    channel_type,
    name_seller_industry AS seller_industry,
    name_yield_group AS yield_group,
    product_combination,
    days_decision AS decision_days,
    days_first_drawing AS first_drawing_days,
    days_first_due AS first_due_days,
    days_last_due AS last_due_days,
    days_termination AS termination_days,
    source_file_name,
    source_file_path,
    pipeline_run_id,
    standardization_timestamp
FROM credit_risk_dev.silver.standardized_previous_application
"""

credit_card_balance_query = """
SELECT
    sk_id_prev AS previous_application_id,
    sk_id_curr AS customer_id,
    months_balance,
    amt_balance AS balance_amount,
    amt_credit_limit_actual AS credit_limit_actual,
    amt_drawings_current AS drawing_amount_current,
    amt_drawings_atm_current AS drawing_amount_atm_current,
    amt_drawings_pos_current AS drawing_amount_pos_current,
    cnt_instalment_mature_cum AS installment_mature_cumulative,
    amt_payment_current AS payment_amount_current,
    amt_payment_total_current AS payment_total_current,
    amt_receivable_principal AS receivable_principal_amount,
    amt_total_receivable AS receivable_total_amount,
    sk_dpd AS days_past_due,
    sk_dpd_def AS days_past_due_def,
    name_contract_status AS contract_status,
    source_file_name,
    source_file_path,
    pipeline_run_id,
    standardization_timestamp
FROM credit_risk_dev.silver.standardized_credit_card_balance
"""

installment_payment_query = """
SELECT
    sk_id_prev AS previous_application_id,
    sk_id_curr AS customer_id,
    num_instalment_version AS installment_version,
    num_instalment_number AS installment_number,
    days_instalment,
    days_entry_payment,
    amt_instalment AS installment_amount,
    amt_payment AS payment_amount,
    source_file_name,
    source_file_path,
    pipeline_run_id,
    standardization_timestamp
FROM credit_risk_dev.silver.standardized_installments_payments
"""

pos_cash_balance_query = """
SELECT
    sk_id_prev AS previous_application_id,
    sk_id_curr AS customer_id,
    months_balance,
    cnt_instalment_future AS installment_future_count,
    cnt_instalment AS installment_count,
    name_contract_status AS contract_status,
    sk_dpd AS days_past_due,
    sk_dpd_def AS days_past_due_def,
    source_file_name,
    source_file_path,
    pipeline_run_id,
    standardization_timestamp
FROM credit_risk_dev.silver.standardized_pos_cash_balance
"""

# COMMAND ----------

now = datetime.now()

rows = [
    Row(
        entity_name="customer_scd2",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_customer_scd2",
        source_query=customer_scd2_query,
        load_strategy="SCD2",
        business_key_columns=["customer_id", "application_source"],
        hash_columns=[
            "gender", "own_car_flag", "own_realty_flag", "children_count",
            "family_members_count", "income_total", "income_type", "education_type",
            "family_status", "housing_type", "occupation_type", "organization_type",
            "days_birth", "days_employed", "region_population_relative"
        ],
        effective_timestamp_column="standardization_timestamp",
        is_scd2=True,
        load_enabled=True,
        load_sequence=1,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="loan_application",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_loan_application",
        source_query=loan_application_query,
        load_strategy="REPLACE_BUSINESS_DT",
        business_key_columns=["business_dt", "customer_id", "application_source"],
        hash_columns=[
            "customer_id", "application_source", "target", "contract_type",
            "credit_amount", "annuity_amount", "goods_price", "income_total",
            "income_type", "education_type", "family_status", "housing_type",
            "occupation_type", "days_birth", "days_employed", "days_registration",
            "days_id_publish"
        ],
        effective_timestamp_column="standardization_timestamp",
        is_scd2=False,
        load_enabled=True,
        load_sequence=2,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="bureau_credit",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_bureau_credit",
        source_query=bureau_credit_query,
        load_strategy="REPLACE_BUSINESS_DT",
        business_key_columns=["business_dt", "bureau_credit_id", "customer_id"],
        hash_columns=[
            "bureau_credit_id", "customer_id", "credit_active", "credit_currency",
            "credit_type", "days_credit", "days_credit_enddate", "days_enddate_fact",
            "credit_day_overdue", "credit_amount", "credit_debt_amount",
            "credit_limit_amount", "credit_overdue_amount", "annuity_amount"
        ],
        effective_timestamp_column="standardization_timestamp",
        is_scd2=False,
        load_enabled=True,
        load_sequence=3,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="previous_application",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_previous_application",
        source_query=previous_application_query,
        load_strategy="REPLACE_BUSINESS_DT",
        business_key_columns=["business_dt", "previous_application_id", "customer_id"],
        hash_columns=[
            "previous_application_id", "customer_id", "contract_type", "annuity_amount",
            "application_amount", "credit_amount", "down_payment_amount", "goods_price",
            "application_status", "payment_type", "rejection_reason", "client_type",
            "goods_category", "portfolio_type", "product_type", "channel_type",
            "seller_industry", "yield_group", "product_combination", "decision_days",
            "first_drawing_days", "first_due_days", "last_due_days", "termination_days"
        ],
        effective_timestamp_column="standardization_timestamp",
        is_scd2=False,
        load_enabled=True,
        load_sequence=4,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="credit_card_balance",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_credit_card_balance",
        source_query=credit_card_balance_query,
        load_strategy="REPLACE_BUSINESS_DT",
        business_key_columns=["business_dt", "customer_id", "previous_application_id", "months_balance"],
        hash_columns=[
            "customer_id", "previous_application_id", "months_balance", "balance_amount",
            "credit_limit_actual", "drawing_amount_current", "drawing_amount_atm_current",
            "drawing_amount_pos_current", "payment_amount_current", "payment_total_current",
            "receivable_principal_amount", "receivable_total_amount", "days_past_due",
            "days_past_due_def", "contract_status"
        ],
        effective_timestamp_column="standardization_timestamp",
        is_scd2=False,
        load_enabled=True,
        load_sequence=5,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="installment_payment",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_installment_payment",
        source_query=installment_payment_query,
        load_strategy="REPLACE_BUSINESS_DT",
        business_key_columns=[
            "business_dt", "customer_id", "previous_application_id",
            "installment_number", "installment_version"
        ],
        hash_columns=[
            "customer_id", "previous_application_id", "installment_version",
            "installment_number", "days_instalment", "days_entry_payment",
            "installment_amount", "payment_amount"
        ],
        effective_timestamp_column="standardization_timestamp",
        is_scd2=False,
        load_enabled=True,
        load_sequence=6,
        created_timestamp=now,
        updated_timestamp=now
    ),
    Row(
        entity_name="pos_cash_balance",
        target_catalog_name="credit_risk_dev",
        target_schema_name="silver",
        target_table_name="conformed_pos_cash_balance",
        source_query=pos_cash_balance_query,
        load_strategy="REPLACE_BUSINESS_DT",
        business_key_columns=["business_dt", "customer_id", "previous_application_id", "months_balance"],
        hash_columns=[
            "customer_id", "previous_application_id", "months_balance",
            "installment_future_count", "installment_count", "contract_status",
            "days_past_due", "days_past_due_def"
        ],
        effective_timestamp_column="standardization_timestamp",
        is_scd2=False,
        load_enabled=True,
        load_sequence=7,
        created_timestamp=now,
        updated_timestamp=now
    )
]

schema = StructType([
    StructField("entity_name", StringType(), False),
    StructField("target_catalog_name", StringType(), False),
    StructField("target_schema_name", StringType(), False),
    StructField("target_table_name", StringType(), False),
    StructField("source_query", StringType(), False),
    StructField("load_strategy", StringType(), False),
    StructField("business_key_columns", ArrayType(StringType()), False),
    StructField("hash_columns", ArrayType(StringType()), False),
    StructField("effective_timestamp_column", StringType(), True),
    StructField("is_scd2", BooleanType(), False),
    StructField("load_enabled", BooleanType(), False),
    StructField("load_sequence", IntegerType(), False),
    StructField("created_timestamp", TimestampType(), False),
    StructField("updated_timestamp", TimestampType(), False),
])

config_df = spark.createDataFrame(rows, schema=schema)

# COMMAND ----------

spark.sql(f"DELETE FROM {config_table}")

(
    config_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(config_table)
)

display(
    spark.sql(f"""
        SELECT entity_name, target_table_name, load_strategy, business_key_columns, load_sequence
        FROM {config_table}
        ORDER BY load_sequence
    """)
)

# COMMAND ----------

# Optional verification: this should show quotes around train/test.
display(
    spark.sql(f"""
        SELECT entity_name, source_query
        FROM {config_table}
        WHERE entity_name IN ('customer_scd2', 'loan_application')
    """)
)
