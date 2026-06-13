-- Databricks notebook source
 SELECT
    to_date('2026-06-02 or ")

-- COMMAND ----------

select * from credit_risk_dev.config.silver_conformance_entity_config


-- COMMAND ----------

select distinct rule_id , target_table_name FROM credit_risk_dev.dq.data_quality_rules

-- COMMAND ----------

select *  FROM credit_risk_dev.dq.data_quality_rules
WHERE target_catalog_name = 'credit_risk_dev'
  AND target_schema_name = 'silver'
  AND target_table_name LIKE 'conformed_%'
  AND rule_id not like '%_CUST_%';



-- COMMAND ----------

delete from credit_risk_dev.bronze.bureau where

-- COMMAND ----------

select * from credit_risk_dev.bronze.bureau limit 2

-- COMMAND ----------

select * from credit_risk_dev.config.silver_column_config where entity_name='bureau'



-- COMMAND ----------

update  credit_risk_dev.config.silver_column_config
set source_column_name = 'sk_id_bureau'
where entity_name='bureau_balance'
and source_column_position = 1

-- COMMAND ----------

update  credit_risk_dev.config.silver_column_config
set source_column_name = 'sk_id_bureau'
where entity_name='bureau'
and source_column_position = 2

-- COMMAND ----------

delete from credit_risk_dev.bronze.application_test      where business_dt='2026-06-02'

-- COMMAND ----------

delete from credit_risk_dev.bronze.application_test      where business_dt is null;
delete from credit_risk_dev.bronze.bureau                where business_dt is null;
delete from credit_risk_dev.bronze.bureau_balance        where business_dt is null;
delete from credit_risk_dev.bronze.previous_application  where business_dt is null;
delete from credit_risk_dev.bronze.pos_cash_balance      where business_dt is null;
delete from credit_risk_dev.bronze.credit_card_balance   where business_dt is null;
delete from credit_risk_dev.bronze.installments_payments where business_dt is null;
delete from credit_risk_dev.bronze.application_train     where business_dt is null;

-- COMMAND ----------

select count(*),business_dt,'credit_risk_dev.bronze.application_test      ' as tgt from credit_risk_dev.bronze.application_test      group by 2,3 union 
select count(*),business_dt,'credit_risk_dev.bronze.bureau                ' as tgt from credit_risk_dev.bronze.bureau                group by 2,3 union 
select count(*),business_dt,'credit_risk_dev.bronze.bureau_balance        ' as tgt from credit_risk_dev.bronze.bureau_balance        group by 2,3 union 
select count(*),business_dt,'credit_risk_dev.bronze.previous_application  ' as tgt from credit_risk_dev.bronze.previous_application  group by 2,3 union 
select count(*),business_dt,'credit_risk_dev.bronze.pos_cash_balance      ' as tgt from credit_risk_dev.bronze.pos_cash_balance      group by 2,3 union 
select count(*),business_dt,'credit_risk_dev.bronze.credit_card_balance   ' as tgt from credit_risk_dev.bronze.credit_card_balance   group by 2,3 union 
select count(*),business_dt,'credit_risk_dev.bronze.installments_payments ' as tgt from credit_risk_dev.bronze.installments_payments group by 2,3 union 
select count(*),business_dt,'credit_risk_dev.bronze.application_train     ' as tgt from credit_risk_dev.bronze.application_train     group by 2,3 ; 

-- COMMAND ----------


select count(*),business_dt,'credit_risk_dev.silver.standardized_application_test     ' as tbl from credit_risk_dev.silver.standardized_application_test      group by 2,3 union
select count(*),business_dt,'credit_risk_dev.silver.standardized_application_train    ' as tbl from credit_risk_dev.silver.standardized_application_train     group by 2,3 union
select count(*),business_dt,'credit_risk_dev.silver.standardized_bureau               ' as tbl from credit_risk_dev.silver.standardized_bureau                group by 2,3 union
select count(*),business_dt,'credit_risk_dev.silver.standardized_bureau_balance       ' as tbl from credit_risk_dev.silver.standardized_bureau_balance        group by 2,3 union
select count(*),business_dt,'credit_risk_dev.silver.standardized_credit_card_balance  ' as tbl from credit_risk_dev.silver.standardized_credit_card_balance   group by 2,3 union
select count(*),business_dt,'credit_risk_dev.silver.standardized_installments_payments' as tbl from credit_risk_dev.silver.standardized_installments_payments group by 2,3 union
select count(*),business_dt,'credit_risk_dev.silver.standardized_pos_cash_balance     ' as tbl from credit_risk_dev.silver.standardized_pos_cash_balance      group by 2,3 union
select count(*),business_dt,'credit_risk_dev.silver.standardized_previous_application ' as tbl from credit_risk_dev.silver.standardized_previous_application  group by 2,3 ;

-- COMMAND ----------

select count(*),'credit_risk_dev.silver.conformed_bureau_credit           ' as tbl from credit_risk_dev.silver.conformed_bureau_credit            union
select count(*),'credit_risk_dev.silver.conformed_credit_card_balance     ' as tbl from credit_risk_dev.silver.conformed_credit_card_balance      union
select count(*),'credit_risk_dev.silver.conformed_customer_scd2           ' as tbl from credit_risk_dev.silver.conformed_customer_scd2            union
select count(*),'credit_risk_dev.silver.conformed_installment_payment     ' as tbl from credit_risk_dev.silver.conformed_installment_payment      union
select count(*),'credit_risk_dev.silver.conformed_loan_application        ' as tbl from credit_risk_dev.silver.conformed_loan_application         union
select count(*),'credit_risk_dev.silver.conformed_pos_cash_balance        ' as tbl from credit_risk_dev.silver.conformed_pos_cash_balance         union
select count(*),'credit_risk_dev.silver.conformed_previous_application    ' as tbl from credit_risk_dev.silver.conformed_previous_application     ;

-- COMMAND ----------

select * from credit_risk_dev.dq.rejected_records --where source_file_name = 'bureau.csv' --and business_dt is not null

-- COMMAND ----------

select * from credit_risk_dev.information_schema.columns where table_schema = 'bronze' and table_name='bureau'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(dbutils.fs.head('/Volumes/credit_risk_dev/files/vol_logs_home_credit_dev/pipeline/silver/bureau_balance/manual_test_20260602_002.log'))

-- COMMAND ----------

select * from credit_risk_dev.dq.rejected_records where source_file_name='bureau_balance.csv'

-- COMMAND ----------

truncate table credit_risk_dev.bronze.bureau_balance

-- COMMAND ----------

select * from  credit_risk_dev.dq.data_quality_rules

-- COMMAND ----------

select * from credit_risk_dev.information_schema.columns where table_name='standardized_application_test'
and column_name='cnt_fam_members'

-- COMMAND ----------

select * from credit_risk_dev.config.silver_conformance_entity_config

-- COMMAND ----------

select * from credit_risk_dev.config.silver_conformance_entity_config

-- COMMAND ----------

 SELECT
            entity_name,
            derived_column_name,
            derived_sql_expression,
            target_data_type,
            is_active,
            column_sequence
        FROM credit_risk_dev.config.silver_conformance_derived_column_config
        WHERE lower(entity_name) = (lower('credit_card_balance'))
          AND is_active = true
        ORDER BY column_sequence

-- COMMAND ----------

select * from  credit_risk_dev.config.silver_conformance_derived_column_config

-- COMMAND ----------

select * from credit_risk_dev.information_schema.columns where table_name = 'conformed_credit_card_balance'

-- COMMAND ----------

select * from credit_risk_dev.information_schema.columns where table_name = 'standardized_credit_card_balance'
