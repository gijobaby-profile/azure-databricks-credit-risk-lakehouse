# Databricks notebook source
# =====================================================================
# Type        : Databricks notebook 
# Notebook    : notebooks/02_silver/01_generic_silver_standardizer.py
# Purpose     : Metadata-driven Silver standardization, validation and DLQ
# =====================================================================

# COMMAND ----------

import sys
import uuid

# COMMAND ----------

from src.silver.silver_config_reader import (
    get_silver_column_config,
    get_bronze_table_name,
    get_standardized_table_name,
)
from src.silver.silver_standardizer import (
    validate_source_columns,
    build_standardized_dataframe,
    split_valid_rejected,
    remove_duplicates,
)
from src.silver.dq_rule_reader import get_active_dq_rules
from src.silver.dq_rule_validator import apply_business_dq_rules
from src.silver.dq_writer import (
    write_rejected_records,
    write_dq_result,
    write_dq_rule_results,
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

repo_root = "/Workspace/Repos/gijobaby-profile/azure-databricks-credit-risk-lakehouse"
if repo_root not in sys.path:
    sys.path.append(repo_root)

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_risk_dev")
dbutils.widgets.text("entity_name", "")
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("write_mode", "overwrite")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name").strip()
entity_name = dbutils.widgets.get("entity_name").strip()
pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip() or str(uuid.uuid4())
write_mode = dbutils.widgets.get("write_mode").strip().lower()

# COMMAND ----------

if not catalog_name:
    raise ValueError("catalog_name is required")

if not entity_name:
    raise ValueError("entity_name is required")

if write_mode not in ("overwrite", "append"):
    raise ValueError("write_mode must be overwrite or append")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

log_file_path = (
    f"/Volumes/{catalog_name}/files/vol_logs_home_credit_dev/"
    f"pipeline/silver/{entity_name}/{pipeline_run_id}.log"
)

logger = get_logger(
    name="silver_standardization",
    log_file_path=log_file_path
)

log_step(
    logger,
    f"Silver standardization parameters | catalog_name={catalog_name} | "
    f"entity_name={entity_name} | pipeline_run_id={pipeline_run_id} | "
    f"write_mode={write_mode}"
)
log_step(logger, f"Runtime log file path: {log_file_path}")

# COMMAND ----------

config = None
table_load_id = None
records_written = 0
rejected_count = 0

# COMMAND ----------

try:
    bronze_table = get_bronze_table_name(catalog_name, entity_name)
    target_table = get_standardized_table_name(catalog_name, entity_name)

    config = {
        "entity_name": entity_name,
        "source_system": "home_credit",
        "source_path": bronze_table,
        "target_table_full_name": target_table,
    }

    log_step(logger, f"Validating Bronze source table | bronze_table={bronze_table}")
    if not spark.catalog.tableExists(bronze_table):
        raise ValueError(f"Bronze source table does not exist: {bronze_table}")

    log_step(logger, f"Validating Silver target table | target_table={target_table}")
    if not spark.catalog.tableExists(target_table):
        raise ValueError(f"Silver target table does not exist: {target_table}")

    log_step(logger, f"Reading Silver column config | entity_name={entity_name}")
    column_config = get_silver_column_config(spark, catalog_name, entity_name)

    log_step(logger, f"Starting pipeline audit | pipeline_run_id={pipeline_run_id}")
    start_pipeline(spark, catalog_name, pipeline_run_id, entity_name)

    log_step(logger, f"Starting table load audit | target_table={target_table}")
    table_load_id = start_load(spark, catalog_name, pipeline_run_id, config)

    log_step(logger, f"Reading Bronze data | bronze_table={bronze_table}")
    bronze_df = spark.table(bronze_table)

    log_step(logger, "Validating configured source columns exist in Bronze")
    validate_source_columns(bronze_df, column_config)

    total_record_count = bronze_df.count()
    log_step(logger, f"Bronze records read | count={total_record_count}")

    log_step(logger, "Applying schema enforcement, column standardization and datatype casting")
    standardized_df = build_standardized_dataframe(
        bronze_df=bronze_df,
        column_config=column_config,
        pipeline_run_id=pipeline_run_id
    )

    log_step(logger, "Splitting valid and rejected records")
    valid_df, rejected_df = split_valid_rejected(
        standardized_df=standardized_df,
        column_config=column_config,
        catalog_name=catalog_name,
        entity_name=entity_name,
        pipeline_run_id=pipeline_run_id,
        target_table_name=target_table
    )

    log_step(logger, "Reading active business DQ rules")
    dq_rules = get_active_dq_rules(spark=spark, catalog_name=catalog_name, target_schema_name="silver", target_table_name=f"standardized_{entity_name}" )
    log_step(logger, f"Active business DQ rules found | count={len(dq_rules)}")

    log_step(logger, "Applying metadata-driven business DQ rules")
    dq_valid_df, dq_rejected_df, dq_rule_results = apply_business_dq_rules(
        valid_df=valid_df,
        dq_rules=dq_rules,
        catalog_name=catalog_name,
        entity_name=entity_name,
        pipeline_run_id=pipeline_run_id,
        target_table_name=target_table
    )

    all_rejected_df = rejected_df.unionByName(dq_rejected_df)

    log_step(logger, "Writing rejected records to DLQ table if any")
    rejected_count = write_rejected_records(
        rejected_df=all_rejected_df,
        catalog_name=catalog_name,
        mode="append"
    )
    log_step(logger, f"Rejected records count | count={rejected_count}")

    log_step(logger, "Removing duplicates using configured dedup keys")
    final_valid_df = remove_duplicates(dq_valid_df, column_config)

    log_step(logger, f"Writing standardized records | target_table={target_table}")
    (
        final_valid_df.write
        .format("delta")
        .mode(write_mode)
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    records_written = final_valid_df.count()
    log_step(logger, f"Silver records written | count={records_written}")

    log_step(logger, "Writing technical schema/cast DQ result summary")
    technical_rejected_count = rejected_df.count()
    write_dq_result(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        target_table_name=target_table,
        rule_id="SILVER_SCHEMA_CAST_VALIDATION",
        rule_name="required_field_and_cast_validation",
        rule_type="SCHEMA_VALIDATION",
        total_record_count=total_record_count,
        failed_record_count=technical_rejected_count,
        severity="HIGH"
    )

    log_step(logger, "Writing business DQ rule result summaries")
    write_dq_rule_results(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        target_table_name=target_table,
        rule_results=dq_rule_results
    )

    end_load_success(spark, catalog_name, table_load_id, records_written)
    end_pipeline_success(spark, catalog_name, pipeline_run_id, records_written)

    success_message = (
        f"SUCCESS | layer=silver_standardized | entity={entity_name} | "
        f"records_written={records_written} | rejected_records={rejected_count} | "
        f"pipeline_run_id={pipeline_run_id}"
    )
    log_step(logger, success_message)
    close_logger(logger)
    dbutils.notebook.exit(success_message)

except Exception as error:
    logger.exception(f"Silver standardization failed | entity_name={entity_name}")
    print(f"Silver standardization failed | entity_name={entity_name} | error={str(error)}")

    if table_load_id:
        end_load_failure(spark, catalog_name, table_load_id, str(error))

    end_pipeline_failure(spark, catalog_name, pipeline_run_id, str(error))

    log_error(
        spark=spark,
        catalog_name=catalog_name,
        pipeline_run_id=pipeline_run_id,
        config=config,
        error=error,
        failed_record_json=None
    )

    close_logger(logger)
    raise

    
