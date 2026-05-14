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


