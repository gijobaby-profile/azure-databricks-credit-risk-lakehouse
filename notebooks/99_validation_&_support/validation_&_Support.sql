-- Databricks notebook source
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
