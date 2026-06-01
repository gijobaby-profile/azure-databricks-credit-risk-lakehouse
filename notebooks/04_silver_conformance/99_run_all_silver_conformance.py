# Databricks notebook source
# =====================================================================
# Notebook    : notebooks/03_silver_conformance/99_run_all_silver_conformance.py
# Purpose     : Run all Silver Conformance entities in controlled order
# =====================================================================

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_risk_dev")
dbutils.widgets.text("business_dt", "")
dbutils.widgets.text("pipeline_run_id", "")

# COMMAND ----------

import uuid
from datetime import date

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name").strip()
business_dt = dbutils.widgets.get("business_dt").strip() or str(date.today())
pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip() or str(uuid.uuid4())

# COMMAND ----------

# Customer SCD2 has dedicated logic.
customer_result = dbutils.notebook.run(
    "./01_silver_conformed_customer_scd2",
    timeout_seconds=0,
    arguments={
        "catalog_name": catalog_name,
        "business_dt": business_dt,
        "pipeline_run_id": pipeline_run_id,
    }
)
print(customer_result)

# COMMAND ----------

# Non-SCD2 entities use the generic builder.
non_scd2_entities = [
    "loan_application",
    "bureau_credit",
    "previous_application",
    "credit_card_balance",
    "installment_payment",
    "pos_cash_balance",
]

for entity_name in non_scd2_entities:
    print(f"Running generic conformance builder | entity={entity_name} | business_dt={business_dt}")
    result = dbutils.notebook.run(
        "./02_generic_silver_conformed_entity",
        timeout_seconds=0,
        arguments={
            "catalog_name": catalog_name,
            "entity_name": entity_name,
            "business_dt": business_dt,
            "pipeline_run_id": pipeline_run_id,
        }
    )
    print(result)

# COMMAND ----------

dbutils.notebook.exit(
    f"SUCCESS | silver_conformance_all | business_dt={business_dt} | pipeline_run_id={pipeline_run_id}"
)
