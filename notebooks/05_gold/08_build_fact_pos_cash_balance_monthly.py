# Databricks notebook source
# MAGIC %md
# MAGIC # Build fact_pos_cash_balance_monthly

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

entity_name = "fact_pos_cash_balance_monthly"
target_table_full_name = f"{catalog_name}.gold.fact_pos_cash_balance_monthly"

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

log_file_path = (
    f"/Volumes/{catalog_name}/files/vol_logs_home_credit_dev/"
    f"pipeline/gold/fact_pos_cash_balance_monthly/{business_dt}/{pipeline_run_id}.log"
)
logger = get_logger(name=f"gold_fact_pos_cash_balance_monthly", log_file_path=log_file_path)

config_for_error = {
    "entity_name": entity_name,
    "source_system": "home_credit",
    "source_path": "silver.conformed_pos_cash_balance",
    "target_table_full_name": target_table_full_name,
}
table_load_id = None

# COMMAND ----------

sql_statement = f"""

INSERT INTO {target_table_full_name}
(
		customer_dim_key 
	, customer_id 
	, previous_application_id 
	, months_balance 
	, installment_future_count 
	, installment_count 
	, days_past_due 
	, days_past_due_def 
	, has_dpd 
	, has_default_dpd 
	, is_active_contract 
	, remaining_installment_ratio 
	, business_dt 
	, pipeline_run_id 
	, gold_created_timestamp 
	, gold_created_date
)
SELECT
    dc.customer_dim_key,
    pcb.customer_id,
    pcb.previous_application_id,
    pcb.months_balance,
    pcb.installment_future_count,
    pcb.installment_count,
    pcb.days_past_due,
    pcb.days_past_due_def,
    pcb.has_dpd,
    pcb.has_default_dpd,
    pcb.is_active_contract,
    pcb.remaining_installment_ratio,
    pcb.business_dt,
    '{escape_sql(pipeline_run_id)}' AS pipeline_run_id,
    current_timestamp(),
    current_date()
	
FROM {catalog_name}.silver.conformed_pos_cash_balance pcb

LEFT JOIN {catalog_name}.gold.dim_customer dc
    ON pcb.customer_id = dc.customer_id
   AND pcb.business_dt = dc.business_dt
   
WHERE pcb.business_dt = DATE('{escape_sql(business_dt)}')

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

    log_step(logger, f"Ending popeline process")
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
