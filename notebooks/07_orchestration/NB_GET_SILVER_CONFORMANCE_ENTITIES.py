# Databricks notebook source
#-----------------------------------------------------------------------------#
# Read active silver conformance entity names from the config table and 
# return them as JSON for Azure Data Factory ForEach.

# ADF ForEach items expression:
# @json(activity('NB_GET_SILVER_CONFORMANCE_ENTITIES').output.runOutput).entities
#-----------------------------------------------------------------------------#

# COMMAND ----------

import json

# COMMAND ----------

dbutils.widgets.text("p_catalog_name", "credit_risk_dev")

catalog_name = dbutils.widgets.get("p_catalog_name").strip()

# COMMAND ----------

if not catalog_name:
    raise ValueError("p_catalog_name is mandatory")

# COMMAND ----------

config_table = f"{catalog_name}.config.silver_conformance_entity_config"

# COMMAND ----------

entity_df = spark.sql(f"""
SELECT DISTINCT
    entity_name
FROM {config_table}
WHERE entity_name IS NOT NULL
ORDER BY entity_name
""")

entities = [{"entity_name": r["entity_name"]} for r in entity_df.collect()]

if len(entities) == 0:
    raise ValueError(f"No silver conformance entities found in {config_table}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "entity_count": len(entities),
    "entities": entities
}))
