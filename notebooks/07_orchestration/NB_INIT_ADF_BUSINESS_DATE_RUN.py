# Databricks notebook source
from src.utils.orchestration_utils import sql_string, sql_date

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_risk_dev")
dbutils.widgets.text("business_dt", "")
dbutils.widgets.text("business_dt_token", "")
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("adf_run_id", "")
dbutils.widgets.text("pipeline_name", "PL_00_TW_MASTER_ORCHESTRATOR")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("source_system", "home_credit")
dbutils.widgets.text("expected_frequency", "DAILY")
dbutils.widgets.text("run_scope", "FULL")
dbutils.widgets.text("run_type", "SCHEDULED")
dbutils.widgets.text("force_reload", "false")
dbutils.widgets.text("requested_by", "ADF")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
business_dt = dbutils.widgets.get("business_dt")
business_dt_token = dbutils.widgets.get("business_dt_token")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
adf_run_id = dbutils.widgets.get("adf_run_id")
pipeline_name = dbutils.widgets.get("pipeline_name")
environment = dbutils.widgets.get("environment")
source_system = dbutils.widgets.get("source_system")
expected_frequency = dbutils.widgets.get("expected_frequency").upper()
run_scope = dbutils.widgets.get("run_scope")
run_type = dbutils.widgets.get("run_type")
force_reload = dbutils.widgets.get("force_reload").lower() == "true"
requested_by = dbutils.widgets.get("requested_by")

# COMMAND ----------

if business_dt_token == "":
    business_dt_token = business_dt.replace("-", "")

# COMMAND ----------

pipeline_run_control = f"{catalog_name}.orchestration.pipeline_run_control"
file_ingestion_metadata = f"{catalog_name}.orchestration.file_ingestion_metadata"
file_arrival_status = f"{catalog_name}.orchestration.file_arrival_status"
layer_processing_status = f"{catalog_name}.orchestration.layer_processing_status"

# COMMAND ----------

# 1. Insert one pipeline run control row.
spark.sql(f"""
    INSERT INTO {pipeline_run_control}
    (
        business_dt,
        pipeline_run_id,
        adf_run_id,
        pipeline_name,
        environment,
        run_scope,
        run_type,
        source_system,
        entity_name,
        file_pattern_name,
        data_file_name,
        layer_name,
        force_reload,
        overall_status,
        start_timestamp,
        end_timestamp,
        requested_by,
        error_message,
        created_timestamp,
        updated_timestamp
    )
    VALUES
    (
        {sql_date(business_dt)},
        {sql_string(pipeline_run_id)},
        {sql_string(adf_run_id)},
        {sql_string(pipeline_name)},
        {sql_string(environment)},
        {sql_string(run_scope)},
        {sql_string(run_type)},
        {sql_string(source_system)},
        NULL,
        NULL,
        NULL,
        NULL,
        {str(force_reload).lower()},
        'RUNNING',
        current_timestamp(),
        NULL,
        {sql_string(requested_by)},
        NULL,
        current_timestamp(),
        current_timestamp()
    )
""")

# COMMAND ----------

# 2. Insert expected file status rows from file_ingestion_metadata.
spark.sql(f"""
    INSERT INTO {file_arrival_status}
    (
        business_dt,
        pipeline_run_id,
        file_id,
        source_system,
        entity_name,
        file_pattern_name,
        resolved_source_base_path,
        resolved_data_file_pattern,
        file_matching_mode,
        matched_data_file_count,
        matched_success_file_count,
        missing_success_file_count,
        data_file_arrived_flag,
        success_file_arrived_flag,
        total_data_file_size_bytes,
        expected_arrival_timestamp,
        sla_deadline_timestamp,
        sla_status,
        arrival_status,
        poll_count,
        retry_count,
        last_checked_timestamp,
        error_message,
        created_timestamp,
        updated_timestamp
    )
    SELECT
        {sql_date(business_dt)} AS business_dt,
        {sql_string(pipeline_run_id)} AS pipeline_run_id,
        file_id,
        source_system,
        entity_name,
        file_pattern_name,
        replace(
            replace(source_base_path_pattern, '{{business_dt}}', {sql_string(business_dt)}),
            '{{business_dt_token}}',
            {sql_string(business_dt_token)}
        ) AS resolved_source_base_path,
        data_file_name_pattern AS resolved_data_file_pattern,
        file_matching_mode,
        0,
        0,
        0,
        false,
        false,
        0,
        NULL,
        NULL,
        'NOT_APPLICABLE',
        'WAITING',
        0,
        0,
        current_timestamp(),
        NULL,
        current_timestamp(),
        current_timestamp()
    FROM {file_ingestion_metadata}
    WHERE active_flag = true
      AND source_system = {sql_string(source_system)}
      AND expected_frequency = {sql_string(expected_frequency)}
""")

# COMMAND ----------

# 3. Insert expected Silver Standardization layer status rows.
spark.sql(f"""
    INSERT INTO {layer_processing_status}
    (
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
    SELECT
        {sql_date(business_dt)} AS business_dt,
        {sql_string(pipeline_run_id)} AS pipeline_run_id,
        source_system,
        entity_name,
        file_pattern_name,
        NULL AS data_file_name,
        'SILVER_STANDARDIZATION' AS layer_name,
        bronze_silver_job_name AS process_name,
        'NOT_STARTED' AS status,
        NULL,
        NULL,
        NULL,
        0,
        0,
        0,
        0,
        {str(force_reload).lower()},
        NULL,
        current_timestamp(),
        current_timestamp()
    FROM {file_ingestion_metadata}
    WHERE active_flag = true
      AND source_system = {sql_string(source_system)}
      AND expected_frequency = {sql_string(expected_frequency)}
""")

# COMMAND ----------

print(f"Initialized {expected_frequency} phase for business_dt={business_dt}, pipeline_run_id={pipeline_run_id}")

dbutils.notebook.exit("SUCCESS")
