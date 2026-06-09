-- Databricks notebook source
Delete from credit_risk_dev.bronze.application_train where business_dt is null;

-- COMMAND ----------

select * from credit_risk_dev.bronze.application_test where business_dt is not null

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def source_path_resolved(source_path: str,entity_name: str , business_dt: str) -> str:
-- MAGIC     source = f"""{source_path}""" 
-- MAGIC     return source
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC result=source_path_resolved('/Volumes/credit_risk_dev/files/vol_landing_home_credit_dev/{entity_name}/{business_dt}','application_train','2026-02-02' )
-- MAGIC
-- MAGIC display(result)

-- COMMAND ----------

SELECT
            entity_name,
            source_system,
            source_path,
            target_table_full_name,
            file_format,
            delimiter,
            header_flag,
            infer_schema_flag,
            merge_schema_flag,
            load_enabled,
            load_sequence
        FROM credit_risk_dev.config.bronze_ingestion_config
        WHERE lower(entity_name) = lower('application_train')
          AND load_enabled = true

-- COMMAND ----------

select * from credit_risk_dev.config.bronze_ingestion_config

-- COMMAND ----------

update  credit_risk_dev.config.bronze_ingestion_config
set source_path = '/Volumes/credit_risk_dev/files/vol_landing_home_credit_dev/{entity_name}/{business_dt}'
where entity_name in ('application_train','application_test','bureau','bureau_balance','previous_application','pos_cash_balance','credit_card_balance','installments_payments')

