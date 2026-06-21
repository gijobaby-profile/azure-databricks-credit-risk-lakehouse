# Databricks notebook source
dbutils.widgets.text("operation_type", "")
dbutils.widgets.text("catalog_name", "credit_risk_dev")

dbutils.widgets.text("business_dt", "")
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("file_id", "")
dbutils.widgets.text("source_system", "home_credit")
dbutils.widgets.text("entity_name", "")
dbutils.widgets.text("file_pattern_name", "")

dbutils.widgets.text("data_file_count", "0")
dbutils.widgets.text("success_file_count", "0")
dbutils.widgets.text("poll_count", "0")
dbutils.widgets.text("mandatory_flag", "true")

dbutils.widgets.text("data_file_name", "")
dbutils.widgets.text("data_file_path", "")
dbutils.widgets.text("success_file_name", "")
dbutils.widgets.text("success_file_path", "")

dbutils.widgets.text("layer_name", "SILVER_STANDARDIZATION")
dbutils.widgets.text("process_name", "")
dbutils.widgets.text("error_message", "")

# COMMAND ----------

operation_type = dbutils.widgets.get("operation_type").upper()
catalog_name = dbutils.widgets.get("catalog_name")

business_dt = dbutils.widgets.get("business_dt")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
file_id = dbutils.widgets.get("file_id")
source_system = dbutils.widgets.get("source_system")
entity_name = dbutils.widgets.get("entity_name")
file_pattern_name = dbutils.widgets.get("file_pattern_name")

data_file_count = int(dbutils.widgets.get("data_file_count") or 0)
success_file_count = int(dbutils.widgets.get("success_file_count") or 0)
poll_count = int(dbutils.widgets.get("poll_count") or 0)
mandatory_flag = dbutils.widgets.get("mandatory_flag").lower() == "true"

data_file_name = dbutils.widgets.get("data_file_name")
data_file_path = dbutils.widgets.get("data_file_path")
success_file_name = dbutils.widgets.get("success_file_name")
success_file_path = dbutils.widgets.get("success_file_path")

layer_name = dbutils.widgets.get("layer_name")
process_name = dbutils.widgets.get("process_name")
error_message = dbutils.widgets.get("error_message")


# COMMAND ----------

file_arrival_status = f"{catalog_name}.orchestration.file_arrival_status"
file_arrival_file_detail = f"{catalog_name}.orchestration.file_arrival_file_detail"
layer_processing_status = f"{catalog_name}.orchestration.layer_processing_status"

# COMMAND ----------

def HandleNull(v):
    if v is None or v == "":
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


# COMMAND ----------

def HandleDate(v):
    return f"DATE({HandleNull(v)})"

# COMMAND ----------

if operation_type == "UPDATE_FILE_WAITING":

    spark.sql(f"""
        UPDATE {file_arrival_status}
        SET arrival_status = 'WAITING',
            poll_count = {poll_count},
            last_checked_timestamp = current_timestamp(),
            updated_timestamp = current_timestamp()
        WHERE business_dt = {HandleDate(business_dt)}
          AND pipeline_run_id = {HandleNull(pipeline_run_id)}
          AND file_id = {HandleNull(file_id)}
    """)

elif operation_type == "UPDATE_FILE_READY":

    spark.sql(f"""
        UPDATE {file_arrival_status}
        SET arrival_status = 'DATA_READY',
            data_file_arrived_flag = true,
            success_file_arrived_flag = true,
            matched_data_file_count = {data_file_count},
            matched_success_file_count = {success_file_count},
            missing_success_file_count = 0,
            poll_count = {poll_count},
            last_checked_timestamp = current_timestamp(),
            updated_timestamp = current_timestamp()
        WHERE business_dt = {HandleDate(business_dt)}
          AND pipeline_run_id = {HandleNull(pipeline_run_id)}
          AND file_id = {HandleNull(file_id)}
    """)

elif operation_type == "UPDATE_FILE_POLLING_STATUS":

    status = "PARTIAL_ARRIVAL" if data_file_count > 0 and success_file_count == 0 else "WAITING"

    spark.sql(f"""
        UPDATE {file_arrival_status}
        SET arrival_status = {HandleNull(status)},
            data_file_arrived_flag = {str(data_file_count > 0).lower()},
            success_file_arrived_flag = {str(success_file_count > 0).lower()},
            matched_data_file_count = {data_file_count},
            matched_success_file_count = {success_file_count},
            missing_success_file_count = CASE WHEN {data_file_count} > {success_file_count}
                                              THEN {data_file_count} - {success_file_count}
                                              ELSE 0 END,
            poll_count = {poll_count},
            last_checked_timestamp = current_timestamp(),
            updated_timestamp = current_timestamp()
        WHERE business_dt = {HandleDate(business_dt)}
          AND pipeline_run_id = {HandleNull(pipeline_run_id)}
          AND file_id = {HandleNull(file_id)}
    """)

elif operation_type == "UPDATE_FILE_TIMEOUT_OR_SKIPPED":

    status = "FAILED" if mandatory_flag else "SKIPPED_OPTIONAL"

    spark.sql(f"""
        UPDATE {file_arrival_status}
        SET arrival_status = {HandleNull(status)},
            poll_count = {poll_count},
            last_checked_timestamp = current_timestamp(),
            error_message = {HandleNull(error_message)},
            updated_timestamp = current_timestamp()
        WHERE business_dt = {HandleDate(business_dt)}
          AND pipeline_run_id = {HandleNull(pipeline_run_id)}
          AND file_id = {HandleNull(file_id)}
    """)

elif operation_type == "INSERT_FILE_ARRIVAL_DETAIL":

    spark.sql(f"""
        INSERT INTO {file_arrival_file_detail}
        (
            business_dt,
            pipeline_run_id,
            file_id,
            source_system,
            entity_name,
            file_pattern_name,
            data_file_name,
            data_file_path,
            data_file_size_bytes,
            data_file_last_modified_ts,
            success_file_name,
            success_file_path,
            success_file_arrived_flag,
            success_file_detected_ts,
            file_status,
            detected_timestamp,
            error_message,
            created_timestamp,
            updated_timestamp
        )
        VALUES
        (
            {HandleDate(business_dt)},
            {HandleNull(pipeline_run_id)},
            {HandleNull(file_id)},
            {HandleNull(source_system)},
            {HandleNull(entity_name)},
            {HandleNull(file_pattern_name)},
            {HandleNull(data_file_name)},
            {HandleNull(data_file_path)},
            0,
            NULL,
            {HandleNull(success_file_name)},
            {HandleNull(success_file_path)},
            true,
            current_timestamp(),
            'READY',
            current_timestamp(),
            NULL,
            current_timestamp(),
            current_timestamp()
        )
    """)

elif operation_type == "UPDATE_LAYER_RUNNING":

    spark.sql(f"""
        UPDATE {layer_processing_status}
        SET status = 'RUNNING',
            process_name = {HandleNull(process_name)},
            start_timestamp = current_timestamp(),
            end_timestamp = NULL,
            error_message = NULL,
            updated_timestamp = current_timestamp()
        WHERE business_dt = {HandleDate(business_dt)}
          AND pipeline_run_id = {HandleNull(pipeline_run_id)}
          AND source_system = {HandleNull(source_system)}
          AND entity_name = {HandleNull(entity_name)}
          AND file_pattern_name = {HandleNull(file_pattern_name)}
          AND layer_name = {HandleNull(layer_name)}
    """)

elif operation_type == "UPDATE_LAYER_SUCCESS":

    spark.sql(f"""
        UPDATE {layer_processing_status}
        SET status = 'SUCCESS',
            end_timestamp = current_timestamp(),
            error_message = NULL,
            updated_timestamp = current_timestamp()
        WHERE business_dt = {HandleDate(business_dt)}
          AND pipeline_run_id = {HandleNull(pipeline_run_id)}
          AND source_system = {HandleNull(source_system)}
          AND entity_name = {HandleNull(entity_name)}
          AND file_pattern_name = {HandleNull(file_pattern_name)}
          AND layer_name = {HandleNull(layer_name)}
    """)

elif operation_type == "UPDATE_LAYER_FAILED":

    spark.sql(f"""
        UPDATE {layer_processing_status}
        SET status = 'FAILED',
            end_timestamp = current_timestamp(),
            error_message = {HandleNull(error_message)},
            updated_timestamp = current_timestamp()
        WHERE business_dt = {HandleDate(business_dt)}
          AND pipeline_run_id = {HandleNull(pipeline_run_id)}
          AND source_system = {HandleNull(source_system)}
          AND entity_name = {HandleNull(entity_name)}
          AND file_pattern_name = {HandleNull(file_pattern_name)}
          AND layer_name = {HandleNull(layer_name)}
    """)

else:
    raise ValueError(f"Unsupported operation_type: {operation_type}")

print(f"Completed operation_type={operation_type}, entity={entity_name}, file_id={file_id}")

dbutils.notebook.exit("SUCCESS")
