-- Databricks notebook source
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

select count(*),business_dt, 'credit_risk_dev.gold.dim_loan_application            ' as tbl from credit_risk_dev.gold.dim_loan_application             group by 2,3 union all
select count(*),business_dt, 'credit_risk_dev.gold.dim_bureau_credit               ' as tbl from credit_risk_dev.gold.dim_bureau_credit                group by 2,3 union all
select count(*),business_dt, 'credit_risk_dev.gold.dim_customer                    ' as tbl from credit_risk_dev.gold.dim_customer                     group by 2,3 union all
select count(*),business_dt, 'credit_risk_dev.gold.fact_installment_payment        ' as tbl from credit_risk_dev.gold.fact_installment_payment         group by 2,3 union all
select count(*),business_dt, 'credit_risk_dev.gold.fact_credit_application         ' as tbl from credit_risk_dev.gold.fact_credit_application          group by 2,3 union all
select count(*),business_dt, 'credit_risk_dev.gold.fact_pos_cash_balance_monthly   ' as tbl from credit_risk_dev.gold.fact_pos_cash_balance_monthly    group by 2,3 union all
select count(*),business_dt, 'credit_risk_dev.gold.fact_customer_risk_snapshot     ' as tbl from credit_risk_dev.gold.fact_customer_risk_snapshot      group by 2,3 union all
select count(*),business_dt, 'credit_risk_dev.gold.fact_bureau_credit_exposure     ' as tbl from credit_risk_dev.gold.fact_bureau_credit_exposure      group by 2,3 union all
select count(*),business_dt, 'credit_risk_dev.gold.fact_credit_card_balance_monthly' as tbl from credit_risk_dev.gold.fact_credit_card_balance_monthly group by 2,3 ;

-- COMMAND ----------

select * from  credit_risk_dev.dq.data_quality_rules

-- COMMAND ----------

select * from credit_risk_dev.config.silver_conformance_entity_config

-- COMMAND ----------

select * from  credit_risk_dev.config.silver_conformance_derived_column_config

-- COMMAND ----------

select * from credit_risk_dev.information_schema.tables where table_schema='gold'

-- COMMAND ----------

select * from credit_risk_dev.information_schema.columns where table_name = 'standardized_credit_card_balance'

-- COMMAND ----------

describe detail credit_risk_dev.bronze.application_train
