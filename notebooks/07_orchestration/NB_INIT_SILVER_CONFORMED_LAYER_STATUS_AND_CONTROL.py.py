# Databricks notebook source
dbutils.widgets.text("p_business_dt", "")
dbutils.widgets.text("p_pipeline_run_id", "")
dbutils.widgets.text("p_source_system", "home_credit")
dbutils.widgets.text("p_force_reload", "false")
dbutils.widgets.text("p_catalog_name", "credit_risk_dev")

# COMMAND ----------

business_dt = dbutils.widgets.get("p_business_dt").strip()
pipeline_run_id = dbutils.widgets.get("p_pipeline_run_id").strip()
source_system = dbutils.widgets.get("p_source_system").strip()
force_reload = dbutils.widgets.get("p_force_reload").strip().lower()
catalog_name = dbutils.widgets.get("p_catalog_name").strip()

# COMMAND ----------

if not business_dt:
    raise ValueError("p_business_dt is mandatory")

if not pipeline_run_id:
    raise ValueError("p_pipeline_run_id is mandatory")

if force_reload not in ("true", "false"):
    raise ValueError("p_force_reload must be either true or false")

# COMMAND ----------

# Table Names
config_table = f"{catalog_name}.config.silver_conformance_entity_config"
status_table = f"{catalog_name}.orchestration.layer_processing_status"

# COMMAND ----------

# Read active Silver Conformed entities

silver_conformance_df = spark.sql(f"""
SELECT
    entity_name,
    target_table_name AS process_name
FROM {config_table}
""")

expected_count = silver_conformance_df.count()

if expected_count == 0:
    raise ValueError(f"No active records found in {config_table}")

silver_conformance_df.createOrReplaceTempView("vw_silver_conformance_entities")

# COMMAND ----------

# Create EXPECTED rows

spark.sql(f"""
MERGE INTO {status_table} AS tgt
USING (
    SELECT
        DATE('{business_dt}') AS business_dt,
        '{pipeline_run_id}' AS pipeline_run_id,
        '{source_system}' AS source_system,
        entity_name,
        CAST(NULL AS STRING) AS file_pattern_name,
        CAST(NULL AS STRING) AS data_file_name,
        'silver_conformed' AS layer_name,
        process_name,
        'EXPECTED' AS status,
        CAST(NULL AS STRING) AS databricks_job_run_id,
        CAST(NULL AS TIMESTAMP) AS start_timestamp,
        CAST(NULL AS TIMESTAMP) AS end_timestamp,
        CAST(NULL AS BIGINT) AS records_read,
        CAST(NULL AS BIGINT) AS records_written,
        CAST(NULL AS BIGINT) AS records_rejected,
        0 AS retry_count,
        CAST('{force_reload}' AS BOOLEAN) AS force_reload,
        CAST(NULL AS STRING) AS error_message,
        current_timestamp() AS created_timestamp,
        current_timestamp() AS updated_timestamp
    FROM vw_silver_conformance_entities
) AS src
ON  tgt.business_dt = src.business_dt
AND tgt.pipeline_run_id = src.pipeline_run_id
AND tgt.entity_name = src.entity_name
AND tgt.layer_name = src.layer_name
AND tgt.process_name = src.process_name

WHEN MATCHED THEN UPDATE SET
    tgt.source_system = src.source_system,
    tgt.file_pattern_name = src.file_pattern_name,
    tgt.data_file_name = src.data_file_name,
    tgt.status = src.status,
    tgt.databricks_job_run_id = src.databricks_job_run_id,
    tgt.start_timestamp = src.start_timestamp,
    tgt.end_timestamp = src.end_timestamp,
    tgt.records_read = src.records_read,
    tgt.records_written = src.records_written,
    tgt.records_rejected = src.records_rejected,
    tgt.retry_count = src.retry_count,
    tgt.force_reload = src.force_reload,
    tgt.error_message = src.error_message,
    tgt.updated_timestamp = current_timestamp()

WHEN NOT MATCHED THEN INSERT (
    business_dt,
    pipeline_run_id,
    source_system,
    entity_name,
    file_pattern_name,
    data_file_name,
    layer_name,
    process_name,
    status,
    databricks_job_run_id,
    start_timestamp,
    end_timestamp,
    records_read,
    records_written,
    records_rejected,
    retry_count,
    force_reload,
    error_message,
    created_timestamp,
    updated_timestamp
)
VALUES (
    src.business_dt,
    src.pipeline_run_id,
    src.source_system,
    src.entity_name,
    src.file_pattern_name,
    src.data_file_name,
    src.layer_name,
    src.process_name,
    src.status,
    src.databricks_job_run_id,
    src.start_timestamp,
    src.end_timestamp,
    src.records_read,
    src.records_written,
    src.records_rejected,
    src.retry_count,
    src.force_reload,
    src.error_message,
    src.created_timestamp,
    src.updated_timestamp
)
""")

# COMMAND ----------

result = {
    "business_dt": business_dt,
    "pipeline_run_id": pipeline_run_id,
    "source_system": source_system,
    "layer_name": "silver_conformed",
    "expected_count": expected_count,
    "status": "EXPECTED_ROWS_CREATED"
}

# COMMAND ----------

dbutils.notebook.exit(str(result))
