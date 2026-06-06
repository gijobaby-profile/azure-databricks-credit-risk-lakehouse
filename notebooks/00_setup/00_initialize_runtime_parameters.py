# =====================================================================
# Notebook    : notebooks/00_setup/00_initialize_runtime_parameters.py
# Purpose     : Common runtime parameter initialization for Databricks jobs
# =====================================================================
#
# This notebook defines and reads common widgets used across Bronze,
# Silver Standardization, Silver Conformance, and Gold notebooks.
#
# It is designed to support:
# - Manual Databricks notebook execution
# - Databricks Jobs
# - ADF-triggered Databricks Jobs
# - Future DAB-based deployments
# =====================================================================

# COMMAND ----------

import uuid
from datetime import date

# COMMAND ----------

# =====================================================================
# Widget definitions
# =====================================================================
# These widgets can be overridden by Databricks Jobs or ADF at runtime.
# Defaults are mainly for local/manual testing.
# =====================================================================

dbutils.widgets.text("catalog_name", "credit_risk_dev")
dbutils.widgets.text("entity_name", "")
dbutils.widgets.text("business_dt", str(date.today()))
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("force_reload", "false")
dbutils.widgets.text("write_mode", "overwrite")

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("job_name", "")
dbutils.widgets.text("task_name", "")
dbutils.widgets.text("layer_name", "")
dbutils.widgets.text("debug_mode", "false")

# COMMAND ----------

# =====================================================================
# Read widget values
# =====================================================================

catalog_name = dbutils.widgets.get("catalog_name").strip()
entity_name = dbutils.widgets.get("entity_name").strip()
business_dt = dbutils.widgets.get("business_dt").strip()
pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip()
force_reload_raw = dbutils.widgets.get("force_reload").strip().lower()
write_mode = dbutils.widgets.get("write_mode").strip().lower()

environment = dbutils.widgets.get("environment").strip().lower()
job_name = dbutils.widgets.get("job_name").strip()
task_name = dbutils.widgets.get("task_name").strip()
layer_name = dbutils.widgets.get("layer_name").strip().lower()
debug_mode_raw = dbutils.widgets.get("debug_mode").strip().lower()

# COMMAND ----------

# =====================================================================
# Apply defaults and conversions
# =====================================================================

if not pipeline_run_id:
    pipeline_run_id = str(uuid.uuid4())

force_reload = force_reload_raw in ["true", "1", "yes", "y"]
debug_mode = debug_mode_raw in ["true", "1", "yes", "y"]

if not business_dt:
    business_dt = str(date.today())

if not environment:
    environment = "dev"

# COMMAND ----------

# =====================================================================
# Validation
# =====================================================================

if not catalog_name:
    raise ValueError("catalog_name is required")

valid_write_modes = ["append", "overwrite", "merge", "replace"]
if write_mode and write_mode not in valid_write_modes:
    raise ValueError(
        f"Invalid write_mode: {write_mode}. "
        f"Allowed values: {valid_write_modes}"
    )

valid_environments = ["dev", "uat", "prod"]
if environment not in valid_environments:
    raise ValueError(
        f"Invalid environment: {environment}. "
        f"Allowed values: {valid_environments}"
    )

# entity_name is not mandatory for every notebook.
# Example:
# - Bronze/Silver Standardization needs entity_name.
# - Gold wrapper may not need one entity_name.
# So entity validation should be done in the calling notebook if required.

# COMMAND ----------

# =====================================================================
# Set catalog
# =====================================================================

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# =====================================================================
# Expose common runtime parameters
# =====================================================================

runtime_params = {
    "catalog_name": catalog_name,
    "entity_name": entity_name,
    "business_dt": business_dt,
    "pipeline_run_id": pipeline_run_id,
    "force_reload": force_reload,
    "write_mode": write_mode,
    "environment": environment,
    "job_name": job_name,
    "task_name": task_name,
    "layer_name": layer_name,
    "debug_mode": debug_mode,
}

# COMMAND ----------

# =====================================================================
# Print runtime parameters
# =====================================================================

print("Runtime parameters initialized")
print(f"catalog_name      = {catalog_name}")
print(f"entity_name       = {entity_name}")
print(f"business_dt       = {business_dt}")
print(f"pipeline_run_id   = {pipeline_run_id}")
print(f"force_reload      = {force_reload}")
print(f"write_mode        = {write_mode}")
print(f"environment       = {environment}")
print(f"job_name          = {job_name}")
print(f"task_name         = {task_name}")
print(f"layer_name        = {layer_name}")
print(f"debug_mode        = {debug_mode}")