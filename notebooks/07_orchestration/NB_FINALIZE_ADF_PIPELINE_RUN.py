# Databricks notebook source
from src.utils.orchestration_utils import sql_string, sql_date

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_risk_dev")
dbutils.widgets.text("business_dt", "")
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("final_status", "SUCCESS")
dbutils.widgets.text("error_message", "")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
business_dt = dbutils.widgets.get("business_dt")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
final_status = dbutils.widgets.get("final_status")
error_message = dbutils.widgets.get("error_message")

# COMMAND ----------

pipeline_run_control = f"{catalog_name}.orchestration.pipeline_run_control"

# COMMAND ----------

spark.sql(f"""
    UPDATE {pipeline_run_control}
    SET overall_status = {sql_string(final_status)},
        end_timestamp = current_timestamp(),
        error_message = {sql_string(error_message)},
        updated_timestamp = current_timestamp()
    WHERE business_dt = {sql_date(business_dt)}
      AND pipeline_run_id = {sql_string(pipeline_run_id)}
""")

# COMMAND ----------

print(f"Pipeline finalized with status={final_status}")
dbutils.notebook.exit("SUCCESS")
