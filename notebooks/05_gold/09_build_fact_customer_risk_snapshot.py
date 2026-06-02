# Databricks notebook source
# MAGIC %md
# MAGIC # fact_customer_risk_snapshot

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_risk_dev")
dbutils.widgets.text("business_dt", "")
dbutils.widgets.text("pipeline_run_id", "")

# COMMAND ----------

import sys

repo_root = "/Workspace/Repos/gijobaby-profile/azure-databricks-credit-risk-lakehouse"
if repo_root not in sys.path:
    sys.path.append(repo_root)

# COMMAND ----------

from src.gold.gold_sql_executor import (
    build_pipeline_run_id,
    resolve_business_dt_sql,
    delete_business_dt_partition,
    count_business_dt,
)
from src.utils.sql_utils import escape_sql
from src.logging.audit_logger import (
    start_pipeline,
    end_pipeline_success,
    end_pipeline_failure,
    start_load,
    end_load_success,
    end_load_failure,
)
from src.logging.error_logger import log_error
from src.utils.logger_utils import get_logger, log_step, close_logger

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name").strip() or "credit_risk_dev"
business_dt = resolve_business_dt_sql(spark, dbutils.widgets.get("business_dt").strip())
pipeline_run_id = build_pipeline_run_id(dbutils.widgets.get("pipeline_run_id"))

# COMMAND ----------

entity_name = "fact_customer_risk_snapshot"
target_table_full_name = f"{catalog_name}.gold.fact_customer_risk_snapshot"

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

log_file_path = (
    f"/Volumes/{catalog_name}/files/vol_logs_home_credit_dev/"
    f"pipeline/gold/fact_customer_risk_snapshot/{business_dt}/{pipeline_run_id}.log"
)
logger = get_logger(name=f"gold_fact_customer_risk_snapshot", log_file_path=log_file_path)

config_for_error = {
    "entity_name": entity_name,
    "source_system": "home_credit",
    "source_path": "multiple silver conformed tables",
    "target_table_full_name": target_table_full_name,
}
table_load_id = None

# COMMAND ----------

sql_statement = f"""

INSERT INTO {target_table_full_name}
(
    customer_dim_key, customer_id, application_source, application_count, default_flag,
    total_credit_amount, total_annuity_amount, total_income_amount,
    avg_credit_income_ratio, avg_annuity_income_ratio,
    bureau_credit_count, active_bureau_credit_count, total_bureau_credit_amount,
    total_bureau_debt_amount, total_bureau_overdue_amount,
    credit_card_balance_amount, credit_card_limit_amount, max_credit_card_dpd,
    installment_count, late_installment_count, underpayment_count,
    total_installment_amount, total_payment_amount,
    pos_contract_count, max_pos_dpd, active_pos_contract_count,
    risk_segment, business_dt, pipeline_run_id, gold_created_timestamp, gold_created_date
)
WITH app AS (
    SELECT
        customer_id,
        application_source,
        business_dt,
        count(*) AS application_count,
        max(target) AS default_flag,
        sum(credit_amount) AS total_credit_amount,
        sum(annuity_amount) AS total_annuity_amount,
        sum(income_total) AS total_income_amount,
        avg(credit_income_ratio) AS avg_credit_income_ratio,
        avg(annuity_income_ratio) AS avg_annuity_income_ratio
    FROM {catalog_name}.silver.conformed_loan_application
    WHERE business_dt = DATE('{escape_sql(business_dt)}')
    GROUP BY customer_id, application_source, business_dt
),
bureau AS (
    SELECT
        customer_id,
        business_dt,
        count(*) AS bureau_credit_count,
        sum(CASE WHEN is_active_credit THEN 1 ELSE 0 END) AS active_bureau_credit_count,
        sum(credit_amount) AS total_bureau_credit_amount,
        sum(credit_debt_amount) AS total_bureau_debt_amount,
        sum(credit_overdue_amount) AS total_bureau_overdue_amount
    FROM {catalog_name}.silver.conformed_bureau_credit
    WHERE business_dt = DATE('{escape_sql(business_dt)}')
    GROUP BY customer_id, business_dt
),
card AS (
    SELECT
        customer_id,
        business_dt,
        sum(balance_amount) AS credit_card_balance_amount,
        sum(credit_limit_actual) AS credit_card_limit_amount,
        max(days_past_due) AS max_credit_card_dpd
    FROM {catalog_name}.silver.conformed_credit_card_balance
    WHERE business_dt = DATE('{escape_sql(business_dt)}')
    GROUP BY customer_id, business_dt
),
inst AS (
    SELECT
        customer_id,
        business_dt,
        count(*) AS installment_count,
        sum(CASE WHEN is_late_payment THEN 1 ELSE 0 END) AS late_installment_count,
        sum(CASE WHEN is_underpayment THEN 1 ELSE 0 END) AS underpayment_count,
        sum(installment_amount) AS total_installment_amount,
        sum(payment_amount) AS total_payment_amount
    FROM {catalog_name}.silver.conformed_installment_payment
    WHERE business_dt = DATE('{escape_sql(business_dt)}')
    GROUP BY customer_id, business_dt
),
pos AS (
    SELECT
        customer_id,
        business_dt,
        count(*) AS pos_contract_count,
        max(days_past_due) AS max_pos_dpd,
        sum(CASE WHEN is_active_contract THEN 1 ELSE 0 END) AS active_pos_contract_count
    FROM {catalog_name}.silver.conformed_pos_cash_balance
    WHERE business_dt = DATE('{escape_sql(business_dt)}')
    GROUP BY customer_id, business_dt
)
SELECT
    dc.customer_dim_key,
    app.customer_id,
    app.application_source,
    app.application_count,
    app.default_flag,
    CAST(app.total_credit_amount AS DECIMAL(18,2)),
    CAST(app.total_annuity_amount AS DECIMAL(18,2)),
    CAST(app.total_income_amount AS DECIMAL(18,2)),
    CAST(app.avg_credit_income_ratio AS DECIMAL(18,6)),
    CAST(app.avg_annuity_income_ratio AS DECIMAL(18,6)),
    coalesce(bureau.bureau_credit_count, 0),
    coalesce(bureau.active_bureau_credit_count, 0),
    CAST(coalesce(bureau.total_bureau_credit_amount, 0) AS DECIMAL(18,2)),
    CAST(coalesce(bureau.total_bureau_debt_amount, 0) AS DECIMAL(18,2)),
    CAST(coalesce(bureau.total_bureau_overdue_amount, 0) AS DECIMAL(18,2)),
    CAST(coalesce(card.credit_card_balance_amount, 0) AS DECIMAL(18,2)),
    CAST(coalesce(card.credit_card_limit_amount, 0) AS DECIMAL(18,2)),
    coalesce(card.max_credit_card_dpd, 0),
    coalesce(inst.installment_count, 0),
    coalesce(inst.late_installment_count, 0),
    coalesce(inst.underpayment_count, 0),
    CAST(coalesce(inst.total_installment_amount, 0) AS DECIMAL(18,2)),
    CAST(coalesce(inst.total_payment_amount, 0) AS DECIMAL(18,2)),
    coalesce(pos.pos_contract_count, 0),
    coalesce(pos.max_pos_dpd, 0),
    coalesce(pos.active_pos_contract_count, 0),
    CASE
        WHEN app.default_flag = 1 THEN 'Defaulted'
        WHEN coalesce(bureau.total_bureau_overdue_amount, 0) > 0 THEN 'High Risk'
        WHEN coalesce(card.max_credit_card_dpd, 0) > 30 OR coalesce(pos.max_pos_dpd, 0) > 30 THEN 'High Risk'
        WHEN coalesce(inst.late_installment_count, 0) > 0 THEN 'Medium Risk'
        WHEN app.avg_credit_income_ratio >= 5 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS risk_segment,
    app.business_dt,
    '{escape_sql(pipeline_run_id)}' AS pipeline_run_id,
    current_timestamp(),
    current_date()
FROM app

LEFT JOIN {catalog_name}.gold.dim_customer dc
    ON app.customer_id = dc.customer_id
   AND app.application_source = dc.application_source
   AND app.business_dt = dc.business_dt

LEFT JOIN bureau
    ON app.customer_id = bureau.customer_id
   AND app.business_dt = bureau.business_dt

LEFT JOIN card
    ON app.customer_id = card.customer_id
   AND app.business_dt = card.business_dt

LEFT JOIN inst
    ON app.customer_id = inst.customer_id
   AND app.business_dt = inst.business_dt

LEFT JOIN pos
    ON app.customer_id = pos.customer_id
   AND app.business_dt = pos.business_dt
"""

# COMMAND ----------

try:
    log_step(
        logger,
        f"START | layer=gold | entity={entity_name} | business_dt={business_dt} | "
        f"pipeline_run_id={pipeline_run_id}"
    )

    # To start the logging
    log_step(logger, f"Starting pipeline audit | pipeline_run_id={pipeline_run_id} | entity_name= gold_{entity_name}")
    start_pipeline(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        entity_name=f"gold_{entity_name}"
    )


    log_step(logger, f"Starting table load audit | pipeline_run_id={pipeline_run_id}")
    table_load_id = start_load(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        config=config_for_error
    )

    log_step(logger, f"Deleting existing data from {target_table_full_name} for the business_dt = {business_dt}")
    records_deleted = delete_business_dt_partition(
        spark=spark,
        target_table_full_name=target_table_full_name,
        business_dt=business_dt
    )

   
    log_step(logger, f"Starting data insertion")
    spark.sql(sql_statement)
    log_step(logger, f"Completed data insertion")


    records_written = count_business_dt(
        spark=spark,
        target_table_full_name=target_table_full_name,
        business_dt=business_dt
    )
    log_step(logger, f"Count of data inserted = {records_written}")

    log_step(logger, f"Ending data loading process")
    end_load_success(
        spark=spark,
        catalog_name=catalog_name,
        table_load_id=table_load_id,
        records_written=records_written
    )

    log_step(logger, f"Ending pipeline process")
    end_pipeline_success(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        records_written=records_written
    )

    message = (
        f"SUCCESS | layer=gold | entity={entity_name} | business_dt={business_dt} | "
        f"records_deleted={records_deleted} | records_written={records_written} | "
        f"pipeline_run_id={pipeline_run_id}"
    )
    log_step(logger, message)


except Exception as error:
    logger.exception(f"FAILED | layer=gold | entity={entity_name} | business_dt={business_dt}")

    if table_load_id:
        end_load_failure(
            spark=spark,
            catalog_name=catalog_name,
            table_load_id=table_load_id,
            error_message=str(error)
        )

    end_pipeline_failure(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        error_message=str(error)
    )

    log_error(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        config=config_for_error,
        error=error
    )

    close_logger(logger)
    raise

finally:
    close_logger(logger)

dbutils.notebook.exit(message)
