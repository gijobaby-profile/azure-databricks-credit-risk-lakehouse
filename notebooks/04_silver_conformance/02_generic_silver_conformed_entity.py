# Databricks notebook source
# =====================================================================
# Notebook    : notebooks/03_silver_conformance/02_build_conformed_entity.py
# Purpose     : Generic metadata-driven builder for non-SCD2 Silver Conformance entities
# Strategy    : REPLACE_BUSINESS_DT using Delta replaceWhere
# =====================================================================

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_risk_dev")
dbutils.widgets.text("entity_name", "")
dbutils.widgets.text("business_dt", "")
dbutils.widgets.text("pipeline_run_id", "")

# COMMAND ----------

import sys
import uuid
from datetime import date

# COMMAND ----------

repo_root = "/Workspace/Repos/gijobaby-profile/azure-databricks-credit-risk-lakehouse"
if repo_root not in sys.path:
    sys.path.append(repo_root)

# COMMAND ----------

from src.silver_conformance.silver_conformance_config_reader import (
    get_silver_conformance_entity_config,
    get_silver_conformance_derived_column_config,
    get_target_table_full_name,
)
from src.silver_conformance.silver_conformance_transformer import build_conformed_dataframe
from src.silver_conformance.silver_conformance_writer import write_conformed_entity
from src.silver_conformance.silver_conformance_dq_validator import (
    read_active_conformance_dq_rules,
    apply_conformance_dq_rules,
    write_conformance_rejected_records,
    write_conformance_dq_results,
)
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

catalog_name = dbutils.widgets.get("catalog_name").strip()
entity_name = dbutils.widgets.get("entity_name").strip()
business_dt = dbutils.widgets.get("business_dt").strip() or str(date.today())
pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip() or str(uuid.uuid4())

# COMMAND ----------

if not entity_name:
    raise ValueError("entity_name is required")


# COMMAND ----------

log_file_path = (
    f"/Volumes/{catalog_name}/files/vol_logs_home_credit_dev/"
    f"pipeline/silver_conformance/{entity_name}/{business_dt}/{pipeline_run_id}.log"
)
logger = get_logger(name=f"silver_conformance_{entity_name}", log_file_path=log_file_path)

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")

table_load_id = None
config_for_error = None

# COMMAND ----------

try:
    entity_config = get_silver_conformance_entity_config(spark, catalog_name, entity_name)

    if entity_config["is_scd2"]:
        raise ValueError("This generic notebook does not process SCD2 entities. Use 01_build_customer_scd2.py.")

    target_table = get_target_table_full_name(entity_config)

    config_for_error = {
        "entity_name": entity_name,
        "source_system": "home_credit",
        "source_path": "metadata_source_query",
        "target_table_full_name": target_table,
    }

    log_step(logger, f"START | entity={entity_name} | business_dt={business_dt} | target={target_table}")

    start_pipeline(spark, catalog_name, pipeline_run_id, f"silver_conformance_{entity_name}")
    table_load_id = start_load(spark, catalog_name, pipeline_run_id, config_for_error)

    derived_config = get_silver_conformance_derived_column_config(spark, catalog_name, entity_name)

    conformed_df = build_conformed_dataframe(
        spark=spark,
        entity_config=entity_config,
        derived_config=derived_config,
        pipeline_run_id=pipeline_run_id,
        business_dt=business_dt
    )

    dq_rules = read_active_conformance_dq_rules(
        spark=spark,
        catalog_name=catalog_name,
        target_schema_name=entity_config["target_schema_name"],
        target_table_name=entity_config["target_table_name"]
    )

    valid_df, rejected_df, dq_results = apply_conformance_dq_rules(
        df=conformed_df,
        dq_rules=dq_rules,
        catalog_name=catalog_name,
        target_schema_name=entity_config["target_schema_name"],
        target_table_name=entity_config["target_table_name"],
        pipeline_run_id=pipeline_run_id
    )

    rejected_count = write_conformance_rejected_records(rejected_df, catalog_name)

    records_written = write_conformed_entity(
        df=valid_df,
        spark=spark,
        entity_config=entity_config,
        business_dt=business_dt
    )

    write_conformance_dq_results(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        table_name=target_table,
        rule_results=dq_results
    )

    end_load_success(spark, catalog_name, table_load_id, records_written)
    end_pipeline_success(spark, catalog_name, pipeline_run_id, records_written)

    message = (
        f"SUCCESS | layer=silver_conformance | entity={entity_name} | "
        f"business_dt={business_dt} | records_written={records_written} | "
        f"rejected_records={rejected_count} | pipeline_run_id={pipeline_run_id}"
    )
    log_step(logger, message)


except Exception as error:
    logger.exception(f"FAILED | entity={entity_name} | business_dt={business_dt}")

    if table_load_id:
        end_load_failure(spark, catalog_name, table_load_id, str(error))

    end_pipeline_failure(spark, catalog_name, pipeline_run_id, str(error))

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
