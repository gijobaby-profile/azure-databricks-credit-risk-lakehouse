-- Databricks notebook source
select * from credit_risk_dev.config.silver_conformance_entity_config;

-- COMMAND ----------

select * from credit_risk_dev.config.silver_column_config

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from typing import Dict, List
-- MAGIC from src.utils.sql_utils import escape_sql
-- MAGIC def get_silver_conformance_entity_config(spark, catalog_name: str, entity_name: str) -> Dict:
-- MAGIC     rows = spark.sql(f"""
-- MAGIC         SELECT
-- MAGIC             entity_name,
-- MAGIC             target_catalog_name,
-- MAGIC             target_schema_name,
-- MAGIC             target_table_name,
-- MAGIC             source_query,
-- MAGIC             load_strategy,
-- MAGIC             business_key_columns,
-- MAGIC             hash_columns,
-- MAGIC             effective_timestamp_column,
-- MAGIC             is_scd2,
-- MAGIC             load_enabled,
-- MAGIC             load_sequence
-- MAGIC         FROM {catalog_name}.config.silver_conformance_entity_config
-- MAGIC         WHERE lower(entity_name) = lower('{escape_sql(entity_name)}')
-- MAGIC           AND load_enabled = true
-- MAGIC     """).collect()
-- MAGIC
-- MAGIC     if not rows:
-- MAGIC         raise ValueError(f"No active Silver Conformance config found for entity_name={entity_name}")
-- MAGIC
-- MAGIC     return row.asDict() for row in rows
-- MAGIC
-- MAGIC get_silver_conformance_entity_config(spark,'credit_risk_dev','customer_scd2')
