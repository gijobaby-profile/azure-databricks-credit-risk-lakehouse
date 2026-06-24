# Databricks notebook source
import json

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_risk_dev")
dbutils.widgets.text("source_system", "home_credit")
dbutils.widgets.text("expected_frequency", "MONTHLY")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
source_system = dbutils.widgets.get("source_system")
expected_frequency = dbutils.widgets.get("expected_frequency").upper()

# COMMAND ----------

metadata_table = f"{catalog_name}.orchestration.file_ingestion_metadata"

# COMMAND ----------

query = f"""
SELECT
    file_id,
    source_system,
    entity_name,
    file_pattern_name,
    source_base_path_pattern,
    data_file_name_pattern,
    success_file_name_suffix,
    mandatory_flag,
    polling_interval_minutes,
    max_poll_count,
    load_sequence
FROM {metadata_table}
WHERE active_flag = true
  AND source_system = '{source_system}'
  AND expected_frequency = '{expected_frequency}'
ORDER BY load_sequence
"""

df = spark.sql(query)




# COMMAND ----------

rows = [row.asDict() for row in df.collect() ]

print("===== SELECTED FILE DETAILS =====")
display(rows)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(rows))
