# Databricks notebook source
# MAGIC %md
# MAGIC # Build fact_bureau_credit_exposure

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

entity_name = "fact_bureau_credit_exposure"
target_table_full_name = f"{catalog_name}.gold.fact_bureau_credit_exposure"

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

log_file_path = (
    f"/Volumes/{catalog_name}/files/vol_logs_home_credit_dev/"
    f"pipeline/gold/fact_bureau_credit_exposure/{business_dt}/{pipeline_run_id}.log"
)
logger = get_logger(name=f"gold_fact_bureau_credit_exposure", log_file_path=log_file_path)

config_for_error = {
    "entity_name": entity_name,
    "source_system": "home_credit",
    "source_path": "silver.conformed_bureau_credit + gold.dim_*",
    "target_table_full_name": target_table_full_name,
}

# COMMAND ----------

table_load_id = None

# COMMAND ----------

sql_statement = f"""

INSERT INTO {target_table_full_name}
(
	customer_dim_key 
	, bureau_credit_dim_key 
	, bureau_credit_id 
	, customer_id 
	, bureau_credit_count 
	, credit_amount 
	, credit_debt_amount 
	, credit_limit_amount 
	, credit_overdue_amount 
	, annuity_amount 
	, debt_to_credit_ratio 
	, has_overdue 
	, business_dt 
	, pipeline_run_id 
	, gold_created_timestamp 
	, gold_created_date
)
SELECT
    dc.customer_dim_key,
    dbc.bureau_credit_dim_key,
    bc.bureau_credit_id,
    bc.customer_id,
    CAST(1 AS BIGINT) AS bureau_credit_count,
    bc.credit_amount,
    bc.credit_debt_amount,
    bc.credit_limit_amount,
    bc.credit_overdue_amount,
    bc.annuity_amount,
    bc.debt_to_credit_ratio,
    bc.has_overdue,
    bc.business_dt,
    '{escape_sql(pipeline_run_id)}' AS pipeline_run_id,
    current_timestamp(),
    current_date()
	
FROM {catalog_name}.silver.conformed_bureau_credit bc

LEFT JOIN {catalog_name}.gold.dim_customer dc
    ON bc.customer_id = dc.customer_id
   AND bc.business_dt = dc.business_dt
   
LEFT JOIN {catalog_name}.gold.dim_bureau_credit dbc
    ON bc.bureau_credit_id = dbc.bureau_credit_id
   AND bc.customer_id = dbc.customer_id
   AND bc.business_dt = dbc.business_dt
   
WHERE bc.business_dt = DATE('{escape_sql(business_dt)}')

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
