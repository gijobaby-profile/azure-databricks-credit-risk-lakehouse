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
from src.silver.dq_writer import (
    write_rejected_records,
    write_dq_result,
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
    
