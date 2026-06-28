# Databricks notebook source
from pyspark.sql import functions as F
import json
from datetime import datetime, timezone

# COMMAND ----------

dbutils.widgets.text("p_business_dt", "")
dbutils.widgets.text("p_layer_name", "silver_standardization")
dbutils.widgets.text("p_catalog_name", "credit_risk_dev")
dbutils.widgets.text("p_expected_frequency", "monthly")

# COMMAND ----------

p_business_dt = dbutils.widgets.get("p_business_dt").strip()
p_layer_name = dbutils.widgets.get("p_layer_name").strip()
p_catalog_name = dbutils.widgets.get("p_catalog_name").strip()
p_expected_frequency = dbutils.widgets.get("p_expected_frequency").strip().lower()

# COMMAND ----------

if not p_business_dt:
    raise ValueError("p_business_dt is mandatory.")

if not p_layer_name:
    raise ValueError("p_layer_name is mandatory.")

if not p_catalog_name:
    raise ValueError("p_catalog_name is mandatory.")

if not p_expected_frequency:
    raise ValueError("p_expected_frequency is mandatory. Example: monthly")

# COMMAND ----------

p_status_table = f"{p_catalog_name}.orchestration.layer_processing_status"
p_metadata_table = f"{p_catalog_name}.orchestration.file_ingestion_metadata"

# COMMAND ----------

# Expected files from metadata table.

expected_files_sql = f"""
SELECT DISTINCT
       CAST(entity_name AS STRING) AS entity_name
FROM {p_metadata_table}
WHERE lower(expected_frequency) = '{p_expected_frequency}'
"""

expected_files_df = spark.sql(expected_files_sql)

expected_file_ids = [row["entity_name"] for row in expected_files_df.collect()]
expected_file_count = len(expected_file_ids)

# COMMAND ----------

if expected_file_count == 0:
    raise ValueError(
        f"No expected files found for expected_frequency = '{p_expected_frequency}' "
        f"in {p_metadata_table}"
    )

# COMMAND ----------

# Create SQL IN list for expected file IDs.
expected_file_ids_sql = ",".join([f"'{entity_name}'" for entity_name in expected_file_ids])

# COMMAND ----------

# Check layer processing status for the selected layer, business date, and expected files.

status_summary_sql = f"""
SELECT
    COUNT(DISTINCT entity_name) AS actual_count,

    COUNT(DISTINCT CASE
        WHEN upper(status) = 'SUCCESS' THEN entity_name
    END) AS success_count,

    COUNT(DISTINCT CASE
        WHEN upper(status) = 'FAILED' THEN entity_name
    END) AS failed_count,

    COUNT(DISTINCT CASE
        WHEN upper(status) IN ('WAITING', 'IN_PROGRESS', 'EXPECTED', 'READY') THEN entity_name
    END) AS waiting_count

FROM {p_status_table}
WHERE business_dt = '{p_business_dt}'
  AND layer_name = '{p_layer_name}'
  AND entity_name IN ({expected_file_ids_sql})
"""

agg_row = spark.sql(status_summary_sql).collect()[0]

actual_count = int(agg_row["actual_count"] or 0)
success_count = int(agg_row["success_count"] or 0)
failed_count = int(agg_row["failed_count"] or 0)
waiting_count = int(agg_row["waiting_count"] or 0)
missing_count = max(expected_file_count - actual_count, 0)

# COMMAND ----------

if failed_count > 0:
    overall_status = "FAILED"
elif success_count >= expected_file_count:
    overall_status = "READY"
else:
    overall_status = "WAITING"

# COMMAND ----------

# Keep not-ready files simple for ADF/debugging.

not_ready_files_sql = f"""
SELECT
    CAST(entity_name AS STRING) AS entity_name,
    CAST(entity_name AS STRING) AS file_name,
    CAST(entity_name AS STRING) AS entity_name,
    CAST(status AS STRING) AS status
FROM {p_status_table}
WHERE business_dt = '{p_business_dt}'
  AND layer_name = '{p_layer_name}'
  AND entity_name IN ({expected_file_ids_sql})
  AND upper(status) <> 'SUCCESS'
LIMIT 50
"""

not_ready_files = [row.asDict() for row in spark.sql(not_ready_files_sql).collect()]

# COMMAND ----------

# Identify missing files using SQL result + Python set difference.

actual_files_sql = f"""
SELECT DISTINCT
       CAST(entity_name AS STRING) AS entity_name
FROM {p_status_table}
WHERE business_dt = '{p_business_dt}'
  AND layer_name = '{p_layer_name}'
  AND entity_name IN ({expected_file_ids_sql})
"""

actual_file_ids = [row["entity_name"] for row in spark.sql(actual_files_sql).collect()]
missing_file_ids = sorted(list(set(expected_file_ids) - set(actual_file_ids)))

# COMMAND ----------

result = {
    "overall_status": overall_status,
    "business_dt": p_business_dt,
    "layer_name": p_layer_name,
    "expected_frequency": p_expected_frequency,
    "expected_file_count": expected_file_count,
    "actual_count": actual_count,
    "success_count": success_count,
    "failed_count": failed_count,
    "waiting_count": waiting_count,
    "missing_count": missing_count,
    "not_ready_files": not_ready_files,
    "missing_file_ids": missing_file_ids[:50],
    "checked_ts_utc": datetime.now(timezone.utc).isoformat()
}

# COMMAND ----------

print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
