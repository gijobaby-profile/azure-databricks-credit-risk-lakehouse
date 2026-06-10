-- Databricks notebook source
select table_catalog||'.'||table_schema||'.'||table_name as tbl from credit_risk_dev.information_schema.tables 
where table_schema = 'silver' order by table_name

-- COMMAND ----------

Delete from credit_risk_dev.bronze.application_train where business_dt is null;

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

select count(*),'credit_risk_dev.bronze.application_test      ' as tgt from credit_risk_dev.bronze.application_test      union 
select count(*),'credit_risk_dev.bronze.bureau                ' as tgt from credit_risk_dev.bronze.bureau                union 
select count(*),'credit_risk_dev.bronze.bureau_balance        ' as tgt from credit_risk_dev.bronze.bureau_balance        union 
select count(*),'credit_risk_dev.bronze.previous_application  ' as tgt from credit_risk_dev.bronze.previous_application  union 
select count(*),'credit_risk_dev.bronze.pos_cash_balance      ' as tgt from credit_risk_dev.bronze.pos_cash_balance      union 
select count(*),'credit_risk_dev.bronze.credit_card_balance   ' as tgt from credit_risk_dev.bronze.credit_card_balance   union 
select count(*),'credit_risk_dev.bronze.installments_payments ' as tgt from credit_risk_dev.bronze.installments_payments union 
select count(*),'credit_risk_dev.bronze.application_train     ' as tgt from credit_risk_dev.bronze.application_train     ; 

-- COMMAND ----------

select count(*),'credit_risk_dev.silver.standardized_application_test     ' as tbl from credit_risk_dev.silver.standardized_application_test      union
select count(*),'credit_risk_dev.silver.standardized_application_train    ' as tbl from credit_risk_dev.silver.standardized_application_train     union
select count(*),'credit_risk_dev.silver.standardized_bureau               ' as tbl from credit_risk_dev.silver.standardized_bureau                union
select count(*),'credit_risk_dev.silver.standardized_bureau_balance       ' as tbl from credit_risk_dev.silver.standardized_bureau_balance        union
select count(*),'credit_risk_dev.silver.standardized_column_description   ' as tbl from credit_risk_dev.silver.standardized_column_description    union
select count(*),'credit_risk_dev.silver.standardized_credit_card_balance  ' as tbl from credit_risk_dev.silver.standardized_credit_card_balance   union
select count(*),'credit_risk_dev.silver.standardized_installments_payments' as tbl from credit_risk_dev.silver.standardized_installments_payments union
select count(*),'credit_risk_dev.silver.standardized_pos_cash_balance     ' as tbl from credit_risk_dev.silver.standardized_pos_cash_balance      union
select count(*),'credit_risk_dev.silver.standardized_previous_application ' as tbl from credit_risk_dev.silver.standardized_previous_application  ;

-- COMMAND ----------

select count(*),'credit_risk_dev.silver.conformed_bureau_credit           ' as tbl from credit_risk_dev.silver.conformed_bureau_credit            union
select count(*),'credit_risk_dev.silver.conformed_credit_card_balance     ' as tbl from credit_risk_dev.silver.conformed_credit_card_balance      union
select count(*),'credit_risk_dev.silver.conformed_customer_scd2           ' as tbl from credit_risk_dev.silver.conformed_customer_scd2            union
select count(*),'credit_risk_dev.silver.conformed_installment_payment     ' as tbl from credit_risk_dev.silver.conformed_installment_payment      union
select count(*),'credit_risk_dev.silver.conformed_loan_application        ' as tbl from credit_risk_dev.silver.conformed_loan_application         union
select count(*),'credit_risk_dev.silver.conformed_pos_cash_balance        ' as tbl from credit_risk_dev.silver.conformed_pos_cash_balance         union
select count(*),'credit_risk_dev.silver.conformed_previous_application    ' as tbl from credit_risk_dev.silver.conformed_previous_application     ;

-- COMMAND ----------

truncate table credit_risk_dev.silver.conformed_bureau_credit            ;
truncate table credit_risk_dev.silver.conformed_credit_card_balance      ;
truncate table credit_risk_dev.silver.conformed_customer_scd2            ;
truncate table credit_risk_dev.silver.conformed_installment_payment      ;
truncate table credit_risk_dev.silver.conformed_loan_application         ;
truncate table credit_risk_dev.silver.conformed_pos_cash_balance         ;
truncate table credit_risk_dev.silver.conformed_previous_application     ;
truncate table credit_risk_dev.silver.standardized_application_test      ;
truncate table credit_risk_dev.silver.standardized_application_train     ;
truncate table credit_risk_dev.silver.standardized_bureau                ;
truncate table credit_risk_dev.silver.standardized_bureau_balance        ;
truncate table credit_risk_dev.silver.standardized_column_description    ;
truncate table credit_risk_dev.silver.standardized_credit_card_balance   ;
truncate table credit_risk_dev.silver.standardized_installments_payments ;
truncate table credit_risk_dev.silver.standardized_pos_cash_balance      ;
truncate table credit_risk_dev.silver.standardized_previous_application  ;

-- COMMAND ----------

select * from credit_risk_dev.config.bronze_ingestion_config

-- COMMAND ----------

update  credit_risk_dev.config.bronze_ingestion_config
set source_path = '/Volumes/credit_risk_dev/files/vol_landing_home_credit_dev/{entity_name}/{business_dt}'
where entity_name in ('application_train','application_test','bureau','bureau_balance','previous_application','pos_cash_balance','credit_card_balance','installments_payments')


-- COMMAND ----------

select * from credit_risk_dev.config.bronze_ingestion_config

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(dbutils.fs.head('/Volumes/credit_risk_dev/files/vol_logs_home_credit_dev/pipeline/silver/bureau_balance/manual_test_20260602_001.log'))
