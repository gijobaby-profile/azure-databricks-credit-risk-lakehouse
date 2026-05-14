# Databricks notebook source
# MAGIC %md
# MAGIC #### metadata-driven Bronze ingestion using COPY INTO

# COMMAND ----------

import sys
import uuid

# COMMAND ----------

repo_root = "/Workspace/Repos/gijobaby-profile/azure-databricks-credit-risk-lakehouse"
if repo_root not in sys.path:
    sys.path.append(repo_root)

# COMMAND ----------

# entity_name examples: application_train, bureau, previous_application
dbutils.widgets.text("catalog_name", "credit_risk_dev")
dbutils.widgets.text("entity_name", "")
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("force_reload", "false")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name").strip()
entity_name = dbutils.widgets.get("entity_name").strip()
pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip() or str(uuid.uuid4())
force_reload = dbutils.widgets.get("force_reload").strip().lower() == "true"

# COMMAND ----------

# Import user defined python functions
from src.metadata.config_reader import get_bronze_config
from src.ingestion.copy_into_builder import build_copy_into_sql
from src.logging.audit_logger import (
    start_pipeline,
    end_pipeline_success,
    end_pipeline_failure,
    start_load,
    end_load_success,
    end_load_failure,
)
from src.logging.error_logger import log_error

# COMMAND ----------

if not catalog_name:
    raise ValueError("catalog_name is required")

if not entity_name:
    raise ValueError("entity_name is required")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

config = None
table_load_id = None
records_written = 0

# COMMAND ----------

try:
    # Read the bronze_ingestion_config table to get the table Nmae
    config = get_bronze_config(spark, catalog_name, entity_name)
    target_table = config["target_table_full_name"]

    # To check the target table is already existing or not
    if not spark.catalog.tableExists(target_table):
        raise ValueError(f"Target table does not exist: {target_table}")

    # To start the logging
    start_pipeline(spark, catalog_name, pipeline_run_id, entity_name)
    table_load_id = start_load(spark, catalog_name, pipeline_run_id, config)

    before_count = spark.table(target_table).count()

    copy_sql = build_copy_into_sql(config, pipeline_run_id, force_reload)
    print(copy_sql)

    copy_result_df = spark.sql(copy_sql)
    display(copy_result_df)

    after_count = spark.table(target_table).count()
    records_written = max(after_count - before_count, 0)

    end_load_success(spark, catalog_name, table_load_id, records_written)
    end_pipeline_success(spark, catalog_name, pipeline_run_id, records_written)

    dbutils.notebook.exit(
        f"SUCCESS | entity={entity_name} | records_written={records_written} | pipeline_run_id={pipeline_run_id}"
    )

except Exception as error:
    if table_load_id:
        end_load_failure(spark, catalog_name, table_load_id, str(error))

    end_pipeline_failure(spark, catalog_name, pipeline_run_id, str(error))
    log_error(spark, catalog_name, pipeline_run_id, config, error)

    raise

