# Databricks notebook source
# MAGIC %md
# MAGIC #### metadata-driven Bronze ingestion using COPY INTO

# COMMAND ----------

import sys
import uuid

# COMMAND ----------

# This is to makesure no error when tryin to import python files created in different folders
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
from src.utils.logger_utils import get_logger, log_step, close_logger


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

log_file_path = (
    f"/Volumes/{catalog_name}/files/vol_logs_home_credit_dev/"
    f"pipeline/bronze/{entity_name}/{pipeline_run_id}.log"
)

logger = get_logger(
    name="bronze_ingestion",
    log_file_path=log_file_path
)

log_step(
    logger,
    f"Bronze ingestion parameters | catalog_name={catalog_name} | "
    f"entity_name={entity_name} | pipeline_run_id={pipeline_run_id} | "
    f"force_reload={force_reload}"
)
log_step(logger, f"Runtime log file path: {log_file_path}")

# COMMAND ----------

logger = get_logger("bronze_ingestion")

logger.info(
    f"Bronze ingestion parameters | catalog_name={catalog_name} | "
    f"entity_name={entity_name} | pipeline_run_id={pipeline_run_id} | "
    f"force_reload={force_reload}"
)

# COMMAND ----------

try:
    # Read the bronze_ingestion_config table to get the table Nmae
    log_step(logger, f"Reading active Bronze config | entity_name={entity_name}")
    config = get_bronze_config(spark, catalog_name, entity_name)
    target_table = config["target_table_full_name"]

    # To check the target table is already existing or not
    log_step(logger, f"Validating target table | target_table={target_table}")
    if not spark.catalog.tableExists(target_table):
        raise ValueError(f"Target table does not exist: {target_table}")

    # To start the logging
    log_step(logger, f"Starting pipeline audit | pipeline_run_id={pipeline_run_id}")
    start_pipeline(spark, catalog_name, pipeline_run_id, entity_name)

    log_step(logger, f"Starting table load audit | target_table={target_table}")
    table_load_id = start_load(spark, catalog_name, pipeline_run_id, config)

    log_step(logger, f"Getting pre-load row count | target_table={target_table}")
    before_count = spark.table(target_table).count()
    log_step(logger, f"before table count = {before_count}")

    log_step(logger, "Building COPY INTO SQL")
    copy_sql = build_copy_into_sql(config, pipeline_run_id, force_reload)
    print(copy_sql)
    log_step(logger, f"COPY INTO SQL = {copy_sql}")

    log_step(logger, f"Executing COPY INTO | entity_name={entity_name}")
    copy_result_df = spark.sql(copy_sql)
    display(copy_result_df)

    after_count = spark.table(target_table).count()
    log_step(logger, f"after table count = {after_count}")

    records_written = max(after_count - before_count, 0)

    log_step(logger,
        f"Bronze load completed | entity_name={entity_name} | "
        f"records_written={records_written}"
    )

    end_load_success(spark, catalog_name, table_load_id, records_written)
    end_pipeline_success(spark, catalog_name, pipeline_run_id, records_written)

    success_message = (
        f"SUCCESS | entity={entity_name} | records_written={records_written} | "
        f"pipeline_run_id={pipeline_run_id}"
    )
    log_step(logger, success_message)
    close_logger(logger)
    dbutils.notebook.exit(success_message)

except Exception as error:
    logger.exception(f"Bronze ingestion failed | entity_name={entity_name}")
    if table_load_id:
        end_load_failure(spark, catalog_name, table_load_id, str(error))

    end_pipeline_failure(spark, catalog_name, pipeline_run_id, str(error))

    # Bronze COPY INTO failures are technical/file-level failures.
    # There is no individual failed business record at this stage,
    # so failed_record_json is passed as None.
    log_error(spark=spark, catalog_name=catalog_name, pipeline_run_id=pipeline_run_id, config=config, error=error, failed_record_json=None )

    close_logger(logger)
    raise


# COMMAND ----------

log_dir = f"/Volumes/{catalog_name}/files/vol_logs_home_credit_dev/pipeline/bronze/{entity_name}/"

files = dbutils.fs.ls(log_dir)

latest_file = sorted(files, key=lambda x: x.modificationTime, reverse=True)[0].path

print(f"Latest log file: {latest_file}")
print(dbutils.fs.head(latest_file, 10000))
