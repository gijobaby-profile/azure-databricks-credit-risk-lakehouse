-- Databricks notebook source
select * from credit_risk_dev.silver.standardized_installments_payments

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC df= spark.sql("""select * from credit_risk_dev.dq.data_quality_rules""")
-- MAGIC df1 = df.select( F.to_json(
-- MAGIC F.struct(*[F.col(column_name) for column_name in df.columns])
-- MAGIC ).alias("jsonrec"))
-- MAGIC display(df1)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.explain(True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df.describe())

-- COMMAND ----------

DESCRIBE TABLE credit_risk_dev.config.silver_column_config;

-- COMMAND ----------

SELECT *
FROM credit_risk_dev.config.bronze_ingestion_config
ORDER BY load_sequence;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("""SELECT *
-- MAGIC FROM credit_risk_dev.config.bronze_ingestion_config
-- MAGIC ORDER BY load_sequence;""")

-- COMMAND ----------

select * from credit_risk_dev.config.silver_column_config

-- COMMAND ----------

UPDATE credit_risk_dev.config.silver_column_config
SET reject_on_cast_failure = is_required

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display((df[2].asDict()))

-- COMMAND ----------

SHOW TABLES IN credit_risk_dev.bronze;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED credit_risk_dev.bronze.application_train;

-- COMMAND ----------

select * from credit_risk_dev.bronze.application_train

-- COMMAND ----------

select * from credit_risk_dev.audit.table_load_log

-- COMMAND ----------

describe table  credit_risk_dev.audit.error_log

-- COMMAND ----------

describe table credit_risk_dev.audit.pipeline_run_log

-- COMMAND ----------

select * from credit_risk_dev.audit.table_load_log

-- COMMAND ----------

select * from credit_risk_dev.bronze.application_train

-- COMMAND ----------

SELECT *
FROM credit_risk_dev.audit.pipeline_run_log
ORDER BY created_timestamp DESC
LIMIT 20;

-- COMMAND ----------

        SELECT
            entity_name,
            source_column_name,
            source_column_position,
            target_column_name,
            target_data_type,
            is_required,
            is_dedup_key,
            is_active,
            column_sequence
        FROM credit_risk_dev.config.silver_column_config

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rows = spark.sql("""
-- MAGIC     SELECT *
-- MAGIC     FROM credit_risk_dev.config.silver_column_config
-- MAGIC     WHERE entity_name = 'application_train'
-- MAGIC     LIMIT 2
-- MAGIC """).collect()
-- MAGIC
-- MAGIC type(rows)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(rows)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(rows)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC list1 = [{1,2,3},{4,5,6}]
-- MAGIC for i in list1:
-- MAGIC     display(i) 
-- MAGIC     display("| ")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC list1 = {'name':'G', 'age':22}
-- MAGIC for i in list1:
-- MAGIC     display(list1[i]) 
-- MAGIC     display("| ")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC entities = ["application_train", "bureau", "previous_application"]
-- MAGIC
-- MAGIC for entity in entities:
-- MAGIC     print(entity)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(entity[-1])

-- COMMAND ----------

select * from credit_risk_dev.dq.data_quality_rules

-- COMMAND ----------

ALTER TABLE credit_risk_dev.config.silver_column_config
ADD COLUMNS (
    reject_on_cast_failure BOOLEAN COMMENT 'Whether record should be rejected if source value cannot be cast to target datatype'
);

-- COMMAND ----------

select * from credit_risk_dev.config.silver_column_config

-- COMMAND ----------

update  credit_risk_dev.config.silver_column_config
set reject_on_cast_failure=true

-- COMMAND ----------

select count(*) from credit_risk_dev.bronze.credit_card_balance

-- COMMAND ----------

select *  FROM credit_risk_dev.config.silver_column_config
        WHERE lower(entity_name) = lower('application_test');






-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from typing import List, Dict, Tuple, Optional
-- MAGIC
-- MAGIC from pyspark.sql import DataFrame, Column
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.window import Window
-- MAGIC
-- MAGIC from src.utils.sql_utils import quote_identifier
-- MAGIC from src.utils.sql_utils import escape_sql, bool_to_sql
-- MAGIC
-- MAGIC def get_silver_column_config(spark, catalog_name: str, entity_name: str) -> List[Dict]:
-- MAGIC     if not str(entity_name or "").strip():
-- MAGIC         raise ValueError("entity_name is required")
-- MAGIC
-- MAGIC     rows = spark.sql(f"""
-- MAGIC         SELECT
-- MAGIC             entity_name,
-- MAGIC             source_column_name,
-- MAGIC             source_column_position,
-- MAGIC             target_column_name,
-- MAGIC             target_data_type,
-- MAGIC             is_required,
-- MAGIC             is_dedup_key,
-- MAGIC             is_active,
-- MAGIC             column_sequence,
-- MAGIC             reject_on_cast_failure
-- MAGIC         FROM {catalog_name}.config.silver_column_config
-- MAGIC         WHERE lower(entity_name) = lower('{escape_sql(entity_name)}')
-- MAGIC           AND is_active = true
-- MAGIC         ORDER BY column_sequence
-- MAGIC     """).collect()
-- MAGIC
-- MAGIC     if not rows:
-- MAGIC         raise ValueError(f"No active Silver column config found for entity_name: {entity_name}")
-- MAGIC     
-- MAGIC     # For every Row object in rows, convert that Row into a dictionary.Return the final list of dictionaries.
-- MAGIC     return [row.asDict() for row in rows]
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC df=get_silver_column_config(spark,catalog_name="credit_risk_dev",entity_name="application_test")
-- MAGIC
-- MAGIC display(df)
