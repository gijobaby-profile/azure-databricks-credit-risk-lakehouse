# Databricks notebook source
# =====================================================================
# Notebook    : notebooks/03_silver_conformance/02_build_customer_scd2.py
# Purpose     : Metadata-driven Customer SCD Type 2 builder
# =====================================================================

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_risk_dev")
dbutils.widgets.text("entity_name", "customer_scd2")
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

from src.silver_conformance.scd2_utils import apply_scd2_merge
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

log_file_path = (
    f"/Volumes/{catalog_name}/files/vol_logs_home_credit_dev/"
    f"pipeline/silver_conformance/{entity_name}/{pipeline_run_id}.log"
)

logger = get_logger(name="silver_conformance_customer_scd2", log_file_path=log_file_path)


# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")

table_load_id = None
config_for_error = None

# COMMAND ----------

try:
    log_step(logger, f"Reading active Silver Conformance config | entity_name={entity_name}")
    entity_config = get_silver_conformance_entity_config(spark, catalog_name, entity_name)

    target_table = get_target_table_full_name(entity_config)
    log_step(logger, f"Target Table Name = {target_table}")

    if not entity_config["is_scd2"]:
        raise ValueError(f"Entity {entity_name} is not configured as SCD2")

    config_for_error = {
        "entity_name": entity_name,
        "source_system": "home_credit",
        "source_path": "metadata_source_query",
        "target_table_full_name": target_table,
    }

    log_step(logger, f"Building Customer SCD2 | target={target_table} | pipeline_run_id={pipeline_run_id}")

    log_step(logger, f"Start pipeline run for silver_conformance_customer_scd2.")
    start_pipeline(spark, catalog_name, pipeline_run_id, "silver_conformance_customer_scd2")

    log_step(logger, f"Start table load run for silver_conformance_customer_scd2.")
    table_load_id = start_load(spark, catalog_name, pipeline_run_id, config_for_error)

    log_step(logger, f"Reading Silver Conformance derive column config | entity_name={entity_name}")
    derived_config = get_silver_conformance_derived_column_config(spark, catalog_name, entity_name)

    log_step(logger, f"Building Silver Conformance dataframe | entity_name={entity_name}")
    source_df = build_conformed_dataframe(
        spark=spark,
        entity_config=entity_config,
        derived_config=derived_config,
        pipeline_run_id=pipeline_run_id,
        business_dt=business_dt
    )

    target_schema_name = entity_config["target_schema_name"]
    target_table_name = entity_config["target_table_name"]

    log_step(logger, f"Reading Silver Conformance dq rules | entity_name={entity_name}")
    dq_rules = read_active_conformance_dq_rules(
        spark=spark,
        catalog_name=catalog_name,
        target_schema_name=target_schema_name,
        target_table_name=target_table_name
    )

    log_step(logger, f"Applying Silver Conformance dq rules | entity_name={entity_name}")
    valid_df, rejected_df, dq_results = apply_conformance_dq_rules(
        df=source_df,
        dq_rules=dq_rules,
        catalog_name=catalog_name,
        target_schema_name=target_schema_name,
        target_table_name=target_table_name,
        pipeline_run_id=pipeline_run_id,
        business_dt=business_dt
    )

    log_step(logger, f"Writing Silver Conformance rejected records | entity_name={entity_name}")
    rejected_count = write_conformance_rejected_records(rejected_df, catalog_name)

    log_step(logger, f"Applying SCD2 merge statement | entity_name={entity_name}")
    records_written = apply_scd2_merge(
        spark=spark,
        source_df=valid_df,
        entity_config=entity_config,
        target_table_full_name=target_table,
        pipeline_run_id=pipeline_run_id,
        business_dt=business_dt
    )

    log_step(logger, f"Writing Silver Conformance dq result | entity_name={entity_name}")
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
        f"scd2_records_inserted={records_written} | rejected_records={rejected_count} | "
        f"pipeline_run_id={pipeline_run_id}"
    )
    log_step(logger, message)
    close_logger(logger)
    dbutils.notebook.exit(message)

except Exception as error:
    logger.exception("Customer SCD2 load failed")

    if table_load_id:
        end_load_failure(spark, catalog_name, table_load_id, str(error))

    end_pipeline_failure(spark, catalog_name, pipeline_run_id, str(error))

    log_error(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        config=config_for_error,
        error=error,
        failed_record_json=None
    )

    close_logger(logger)
    raise

