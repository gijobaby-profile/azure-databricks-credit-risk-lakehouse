# Databricks notebook source
# MAGIC %md
# MAGIC # Run all Gold layer notebooks in dependency order

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_risk_dev")
dbutils.widgets.text("business_dt", "")
dbutils.widgets.text("pipeline_run_id", "")

# COMMAND ----------

import uuid
from datetime import date

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name").strip() or "credit_risk_dev"
business_dt = dbutils.widgets.get("business_dt").strip() or str(date.today())
pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip() or str(uuid.uuid4())

# COMMAND ----------

gold_notebooks = [
    "./01_gold_dim_customer",
    "./02_gold_dim_loan_application",
    "./03_gold_dim_bureau_credit",
    "./04_gold_fact_credit_application",
    "./05_gold_fact_bureau_credit_exposure",
    "./06_gold_fact_credit_card_balance_monthly",
    "./07_gold_fact_installment_payment",
    "./08_gold_fact_pos_cash_balance_monthly",
    "./09_gold_fact_customer_risk_snapshot",
]

# COMMAND ----------

for notebook_path in gold_notebooks:
    print(f"Running {notebook_path} | business_dt={business_dt} | pipeline_run_id={pipeline_run_id}")
    result = dbutils.notebook.run(
        notebook_path,
        timeout_seconds=0,
        arguments={
            "catalog_name": catalog_name,
            "business_dt": business_dt,
            "pipeline_run_id": pipeline_run_id,
        },
    )
    print(result)

# COMMAND ----------

dbutils.notebook.exit(
    f"SUCCESS | layer=gold | business_dt={business_dt} | pipeline_run_id={pipeline_run_id}"
)
