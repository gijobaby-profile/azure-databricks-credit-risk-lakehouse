-- Databricks notebook source
Delete from credit_risk_dev.bronze.application_train where business_dt is null;
dev
=======

-- COMMAND ----------

select * from credit_risk_dev.bronze.application_test where business_dt is not null
main

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

select * from credit_risk_dev.config.bronze_ingestion_config

-- COMMAND ----------

update  credit_risk_dev.config.bronze_ingestion_config
set source_path = '/Volumes/credit_risk_dev/files/vol_landing_home_credit_dev/{entity_name}/{business_dt}'
where entity_name in ('application_train','application_test','bureau','bureau_balance','previous_application','pos_cash_balance','credit_card_balance','installments_payments')


-- COMMAND ----------

select * from credit_risk_dev.config.bronze_ingestion_config
