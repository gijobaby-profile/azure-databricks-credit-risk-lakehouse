-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Bronze layer tables

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.bronze.application_train (
  sk_id_curr STRING,
  target STRING,
  name_contract_type STRING,
  code_gender STRING,
  flag_own_car STRING,
  flag_own_realty STRING,
  cnt_children STRING,
  amt_income_total STRING,
  amt_credit STRING,
  amt_annuity STRING,
  amt_goods_price STRING,
  name_type_suite STRING,
  name_income_type STRING,
  name_education_type STRING,
  name_family_status STRING,
  name_housing_type STRING,
  region_population_relative STRING,
  days_birth STRING,
  days_employed STRING,
  days_registration STRING,
  days_id_publish STRING,
  own_car_age STRING,
  flag_mobil STRING,
  flag_emp_phone STRING,
  flag_work_phone STRING,
  flag_cont_mobile STRING,
  flag_phone STRING,
  flag_email STRING,
  occupation_type STRING,
  cnt_fam_members STRING,
  region_rating_client STRING,
  region_rating_client_w_city STRING,
  weekday_appr_process_start STRING,
  hour_appr_process_start STRING,
  reg_region_not_live_region STRING,
  reg_region_not_work_region STRING,
  live_region_not_work_region STRING,
  reg_city_not_live_city STRING,
  reg_city_not_work_city STRING,
  live_city_not_work_city STRING,
  organization_type STRING,
  ext_source_1 STRING,
  ext_source_2 STRING,
  ext_source_3 STRING,
  apartments_avg STRING,
  basementarea_avg STRING,
  years_beginexpluatation_avg STRING,
  years_build_avg STRING,
  commonarea_avg STRING,
  elevators_avg STRING,
  entrances_avg STRING,
  floorsmax_avg STRING,
  floorsmin_avg STRING,
  landarea_avg STRING,
  livingapartments_avg STRING,
  livingarea_avg STRING,
  nonlivingapartments_avg STRING,
  nonlivingarea_avg STRING,
  apartments_mode STRING,
  basementarea_mode STRING,
  years_beginexpluatation_mode STRING,
  years_build_mode STRING,
  commonarea_mode STRING,
  elevators_mode STRING,
  entrances_mode STRING,
  floorsmax_mode STRING,
  floorsmin_mode STRING,
  landarea_mode STRING,
  livingapartments_mode STRING,
  livingarea_mode STRING,
  nonlivingapartments_mode STRING,
  nonlivingarea_mode STRING,
  apartments_medi STRING,
  basementarea_medi STRING,
  years_beginexpluatation_medi STRING,
  years_build_medi STRING,
  commonarea_medi STRING,
  elevators_medi STRING,
  entrances_medi STRING,
  floorsmax_medi STRING,
  floorsmin_medi STRING,
  landarea_medi STRING,
  livingapartments_medi STRING,
  livingarea_medi STRING,
  nonlivingapartments_medi STRING,
  nonlivingarea_medi STRING,
  fondkapremont_mode STRING,
  housetype_mode STRING,
  totalarea_mode STRING,
  wallsmaterial_mode STRING,
  emergencystate_mode STRING,
  obs_30_cnt_social_circle STRING,
  def_30_cnt_social_circle STRING,
  obs_60_cnt_social_circle STRING,
  def_60_cnt_social_circle STRING,
  days_last_phone_change STRING,
  flag_document_2 STRING,
  flag_document_3 STRING,
  flag_document_4 STRING,
  flag_document_5 STRING,
  flag_document_6 STRING,
  flag_document_7 STRING,
  flag_document_8 STRING,
  flag_document_9 STRING,
  flag_document_10 STRING,
  flag_document_11 STRING,
  flag_document_12 STRING,
  flag_document_13 STRING,
  flag_document_14 STRING,
  flag_document_15 STRING,
  flag_document_16 STRING,
  flag_document_17 STRING,
  flag_document_18 STRING,
  flag_document_19 STRING,
  flag_document_20 STRING,
  flag_document_21 STRING,
  amt_req_credit_bureau_hour STRING,
  amt_req_credit_bureau_day STRING,
  amt_req_credit_bureau_week STRING,
  amt_req_credit_bureau_mon STRING,
  amt_req_credit_bureau_qrt STRING,
  amt_req_credit_bureau_year STRING,
  source_file_name STRING,
  source_file_path STRING,
  pipeline_run_id STRING,
  ingestion_timestamp TIMESTAMP,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.bronze.application_test (
  sk_id_curr STRING,
  name_contract_type STRING,
  code_gender STRING,
  flag_own_car STRING,
  flag_own_realty STRING,
  cnt_children STRING,
  amt_income_total STRING,
  amt_credit STRING,
  amt_annuity STRING,
  amt_goods_price STRING,
  name_type_suite STRING,
  name_income_type STRING,
  name_education_type STRING,
  name_family_status STRING,
  name_housing_type STRING,
  region_population_relative STRING,
  days_birth STRING,
  days_employed STRING,
  days_registration STRING,
  days_id_publish STRING,
  own_car_age STRING,
  flag_mobil STRING,
  flag_emp_phone STRING,
  flag_work_phone STRING,
  flag_cont_mobile STRING,
  flag_phone STRING,
  flag_email STRING,
  occupation_type STRING,
  cnt_fam_members STRING,
  region_rating_client STRING,
  region_rating_client_w_city STRING,
  weekday_appr_process_start STRING,
  hour_appr_process_start STRING,
  reg_region_not_live_region STRING,
  reg_region_not_work_region STRING,
  live_region_not_work_region STRING,
  reg_city_not_live_city STRING,
  reg_city_not_work_city STRING,
  live_city_not_work_city STRING,
  organization_type STRING,
  ext_source_1 STRING,
  ext_source_2 STRING,
  ext_source_3 STRING,
  apartments_avg STRING,
  basementarea_avg STRING,
  years_beginexpluatation_avg STRING,
  years_build_avg STRING,
  commonarea_avg STRING,
  elevators_avg STRING,
  entrances_avg STRING,
  floorsmax_avg STRING,
  floorsmin_avg STRING,
  landarea_avg STRING,
  livingapartments_avg STRING,
  livingarea_avg STRING,
  nonlivingapartments_avg STRING,
  nonlivingarea_avg STRING,
  apartments_mode STRING,
  basementarea_mode STRING,
  years_beginexpluatation_mode STRING,
  years_build_mode STRING,
  commonarea_mode STRING,
  elevators_mode STRING,
  entrances_mode STRING,
  floorsmax_mode STRING,
  floorsmin_mode STRING,
  landarea_mode STRING,
  livingapartments_mode STRING,
  livingarea_mode STRING,
  nonlivingapartments_mode STRING,
  nonlivingarea_mode STRING,
  apartments_medi STRING,
  basementarea_medi STRING,
  years_beginexpluatation_medi STRING,
  years_build_medi STRING,
  commonarea_medi STRING,
  elevators_medi STRING,
  entrances_medi STRING,
  floorsmax_medi STRING,
  floorsmin_medi STRING,
  landarea_medi STRING,
  livingapartments_medi STRING,
  livingarea_medi STRING,
  nonlivingapartments_medi STRING,
  nonlivingarea_medi STRING,
  fondkapremont_mode STRING,
  housetype_mode STRING,
  totalarea_mode STRING,
  wallsmaterial_mode STRING,
  emergencystate_mode STRING,
  obs_30_cnt_social_circle STRING,
  def_30_cnt_social_circle STRING,
  obs_60_cnt_social_circle STRING,
  def_60_cnt_social_circle STRING,
  days_last_phone_change STRING,
  flag_document_2 STRING,
  flag_document_3 STRING,
  flag_document_4 STRING,
  flag_document_5 STRING,
  flag_document_6 STRING,
  flag_document_7 STRING,
  flag_document_8 STRING,
  flag_document_9 STRING,
  flag_document_10 STRING,
  flag_document_11 STRING,
  flag_document_12 STRING,
  flag_document_13 STRING,
  flag_document_14 STRING,
  flag_document_15 STRING,
  flag_document_16 STRING,
  flag_document_17 STRING,
  flag_document_18 STRING,
  flag_document_19 STRING,
  flag_document_20 STRING,
  flag_document_21 STRING,
  amt_req_credit_bureau_hour STRING,
  amt_req_credit_bureau_day STRING,
  amt_req_credit_bureau_week STRING,
  amt_req_credit_bureau_mon STRING,
  amt_req_credit_bureau_qrt STRING,
  amt_req_credit_bureau_year STRING,
  source_file_name STRING,
  source_file_path STRING,
  pipeline_run_id STRING,
  ingestion_timestamp TIMESTAMP,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.bronze.bureau (
  sk_id_curr STRING,
  sk_id_bureau STRING,
  credit_active STRING,
  credit_currency STRING,
  days_credit STRING,
  credit_day_overdue STRING,
  days_credit_enddate STRING,
  days_enddate_fact STRING,
  amt_credit_max_overdue STRING,
  cnt_credit_prolong STRING,
  amt_credit_sum STRING,
  amt_credit_sum_debt STRING,
  amt_credit_sum_limit STRING,
  amt_credit_sum_overdue STRING,
  credit_type STRING,
  days_credit_update STRING,
  amt_annuity STRING,
  source_file_name STRING,
  source_file_path STRING,
  pipeline_run_id STRING,
  ingestion_timestamp TIMESTAMP,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.bronze.bureau_balance (
  sk_id_bureau STRING,
  months_balance STRING,
  status STRING,
  source_file_name STRING,
  source_file_path STRING,
  pipeline_run_id STRING,
  ingestion_timestamp TIMESTAMP,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.bronze.previous_application (
  sk_id_prev STRING,
  sk_id_curr STRING,
  name_contract_type STRING,
  amt_annuity STRING,
  amt_application STRING,
  amt_credit STRING,
  amt_down_payment STRING,
  amt_goods_price STRING,
  weekday_appr_process_start STRING,
  hour_appr_process_start STRING,
  flag_last_appl_per_contract STRING,
  nflag_last_appl_in_day STRING,
  nflag_micro_cash STRING,
  rate_down_payment STRING,
  rate_interest_primary STRING,
  rate_interest_privileged STRING,
  name_cash_loan_purpose STRING,
  name_contract_status STRING,
  days_decision STRING,
  name_payment_type STRING,
  code_reject_reason STRING,
  name_type_suite STRING,
  name_client_type STRING,
  name_goods_category STRING,
  name_portfolio STRING,
  name_product_type STRING,
  channel_type STRING,
  sellerplace_area STRING,
  name_seller_industry STRING,
  cnt_payment STRING,
  name_yield_group STRING,
  product_combination STRING,
  days_first_drawing STRING,
  days_first_due STRING,
  days_last_due_1st_version STRING,
  days_last_due STRING,
  days_termination STRING,
  nflag_insured_on_approval STRING,
  source_file_name STRING,
  source_file_path STRING,
  pipeline_run_id STRING,
  ingestion_timestamp TIMESTAMP,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.bronze.pos_cash_balance (
  sk_id_prev STRING,
  sk_id_curr STRING,
  months_balance STRING,
  cnt_instalment STRING,
  cnt_instalment_future STRING,
  name_contract_status STRING,
  sk_dpd STRING,
  sk_dpd_def STRING,
  source_file_name STRING,
  source_file_path STRING,
  pipeline_run_id STRING,
  ingestion_timestamp TIMESTAMP,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.bronze.credit_card_balance (
  sk_id_prev STRING,
  sk_id_curr STRING,
  months_balance STRING,
  amt_balance STRING,
  amt_credit_limit_actual STRING,
  amt_drawings_atm_current STRING,
  amt_drawings_current STRING,
  amt_drawings_other_current STRING,
  amt_drawings_pos_current STRING,
  amt_inst_min_regularity STRING,
  amt_payment_current STRING,
  amt_payment_total_current STRING,
  amt_receivable_principal STRING,
  amt_recivable STRING,
  amt_total_receivable STRING,
  cnt_drawings_atm_current STRING,
  cnt_drawings_current STRING,
  cnt_drawings_other_current STRING,
  cnt_drawings_pos_current STRING,
  cnt_instalment_mature_cum STRING,
  name_contract_status STRING,
  sk_dpd STRING,
  sk_dpd_def STRING,
  source_file_name STRING,
  source_file_path STRING,
  pipeline_run_id STRING,
  ingestion_timestamp TIMESTAMP,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.bronze.installments_payments (
  sk_id_prev STRING,
  sk_id_curr STRING,
  num_instalment_version STRING,
  num_instalment_number STRING,
  days_instalment STRING,
  days_entry_payment STRING,
  amt_instalment STRING,
  amt_payment STRING,
  source_file_name STRING,
  source_file_path STRING,
  pipeline_run_id STRING,
  ingestion_timestamp TIMESTAMP,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_risk_dev.bronze.column_description (
  source_table STRING,
  source_column STRING,
  column_description STRING,
  special_note STRING,
  source_file_name STRING,
  source_file_path STRING,
  pipeline_run_id STRING,
  ingestion_timestamp TIMESTAMP,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date);

-- COMMAND ----------

ALTER TABLE credit_risk_dev.bronze.application_train ADD COLUMNS (business_dt DATE);

-- COMMAND ----------

ALTER TABLE credit_risk_dev.bronze.application_test ADD COLUMNS (business_dt DATE);

-- COMMAND ----------

ALTER TABLE credit_risk_dev.bronze.bureau_balance ADD COLUMNS (business_dt DATE);

-- COMMAND ----------

ALTER TABLE credit_risk_dev.bronze.bureau ADD COLUMNS (business_dt DATE);

-- COMMAND ----------

ALTER TABLE credit_risk_dev.bronze.previous_application ADD COLUMNS (business_dt DATE);

-- COMMAND ----------

ALTER TABLE credit_risk_dev.bronze.pos_cash_balance ADD COLUMNS (business_dt DATE);

-- COMMAND ----------

ALTER TABLE credit_risk_dev.bronze.credit_card_balance ADD COLUMNS (business_dt DATE);

-- COMMAND ----------

ALTER TABLE credit_risk_dev.bronze.installments_payments ADD COLUMNS (business_dt DATE);

-- COMMAND ----------

ALTER TABLE credit_risk_dev.bronze.column_description ADD COLUMNS (business_dt DATE);
