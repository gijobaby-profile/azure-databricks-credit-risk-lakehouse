-- Databricks notebook source
select * from credit_risk_dev.config.silver_conformance_entity_config;

-- COMMAND ----------

select * from credit_risk_dev.config.silver_column_config

-- COMMAND ----------

SELECT
    'customer_scd2',
    'credit_risk_dev',
    'silver',
    'conformed_customer_scd2',
    '
    SELECT DISTINCT
        customer_id,
        gender,
        own_car_flag,
        own_realty_flag,
        children_count,
        family_members_count,
        income_total,
        income_type,
        education_type,
        family_status,
        housing_type,
        occupation_type,
        organization_type,
        days_birth,
        days_employed,
        region_population_relative,
        source_system,
        pipeline_run_id,
        standardization_timestamp
    FROM (
        SELECT
            sk_id_curr AS customer_id,
            code_gender AS gender,
            flag_own_car AS own_car_flag,
            flag_own_realty AS own_realty_flag,
            cnt_children AS children_count,
            cnt_fam_members AS family_members_count,
            amt_income_total AS income_total,
            name_income_type AS income_type,
            name_education_type AS education_type,
            name_family_status AS family_status,
            name_housing_type AS housing_type,
            occupation_type,
            organization_type,
            days_birth,
            days_employed,
            region_population_relative,
            CAST(''train'' as STRING) AS source_system,
            pipeline_run_id,
            standardization_timestamp
        FROM credit_risk_dev.silver.standardized_application_train
        WHERE sk_id_curr IS NOT NULL

        UNION ALL

        SELECT
            sk_id_curr AS customer_id,
            code_gender AS gender,
            flag_own_car AS own_car_flag,
            flag_own_realty AS own_realty_flag,
            cnt_children AS children_count,
            cnt_fam_members AS family_members_count,
            amt_income_total AS income_total,
            name_income_type AS income_type,
            name_education_type AS education_type,
            name_family_status AS family_status,
            name_housing_type AS housing_type,
            occupation_type,
            organization_type,
            days_birth,
            days_employed,
            region_population_relative,
            CAST(''test'' as STRING) AS source_system,
            pipeline_run_id,
            standardization_timestamp
        FROM credit_risk_dev.silver.standardized_application_test
        WHERE sk_id_curr IS NOT NULL
    )
    WHERE customer_id IS NOT NULL
    ',
    'SCD2',
    array('customer_id'),
    array('gender','own_car_flag','own_realty_flag','children_count','family_members_count','income_total','income_type','education_type','family_status','housing_type','occupation_type','organization_type','days_birth','days_employed','region_population_relative'),
    'standardization_timestamp',
    true,
    true,
    1,
    current_timestamp(),
    current_timestamp()

-- COMMAND ----------

''test''
