# Databricks notebook source
# MAGIC %md
# MAGIC # Build dim_customer

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

entity_name = "dim_customer"
target_table_full_name = f"{catalog_name}.gold.dim_customer"

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

log_file_path = (
    f"/Volumes/{catalog_name}/files/vol_logs_home_credit_dev/"
    f"pipeline/gold/dim_customer/{business_dt}/{pipeline_run_id}.log"
)
logger = get_logger(name=f"gold_dim_customer", log_file_path=log_file_path)

config_for_error = {
    "entity_name": entity_name,
    "source_system": "home_credit",
    "source_path": "silver.conformed_customer_scd2",
    "target_table_full_name": target_table_full_name,
}


# COMMAND ----------

table_load_id = None

# COMMAND ----------

sql_statement = f"""

INSERT INTO {target_table_full_name}
(
    customer_id, application_source, gender, own_car_flag, own_realty_flag,
    children_count, family_members_count, income_total, income_type, education_type,
    family_status, housing_type, occupation_type, organization_type,
    age_years, employment_years, region_population_relative,
    scd_effective_from, scd_effective_to, is_current,
    business_dt, source_system, pipeline_run_id, gold_created_timestamp, gold_created_date
)
SELECT
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
    CAST(round(abs(days_birth) / 365.25, 2) AS DECIMAL(10,2)) AS age_years,
    CAST(CASE WHEN days_employed IS NULL THEN NULL ELSE round(abs(days_employed) / 365.25, 2) END AS DECIMAL(10,2)) AS employment_years,
    region_population_relative,
    effective_from AS scd_effective_from,
    effective_to AS scd_effective_to,
    is_current,
    business_dt,
    source_system,
    '{escape_sql(pipeline_run_id)}' AS pipeline_run_id,
    current_timestamp() AS gold_created_timestamp,
    current_date() AS gold_created_date

FROM {catalog_name}.silver.conformed_customer_scd2

WHERE business_dt = DATE('{escape_sql(business_dt)}')
  AND is_current = true
"""

# COMMAND ----------

try:
    log_step(
        logger,
        f"START | layer=gold | entity={entity_name} | business_dt={business_dt} | "
        f"pipeline_run_id={pipeline_run_id}"
    )

    start_pipeline(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        entity_name=f"gold_{entity_name}"
    )

    table_load_id = start_load(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        config=config_for_error
    )

    records_deleted = delete_business_dt_partition(
        spark=spark,
        target_table_full_name=target_table_full_name,
        business_dt=business_dt
    )

    spark.sql(sql_statement)

    records_written = count_business_dt(
        spark=spark,
        target_table_full_name=target_table_full_name,
        business_dt=business_dt
    )

    end_load_success(
        spark=spark,
        catalog_name=catalog_name,
        table_load_id=table_load_id,
        records_written=records_written
    )

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
    close_logger(logger)
    dbutils.notebook.exit(message)

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

