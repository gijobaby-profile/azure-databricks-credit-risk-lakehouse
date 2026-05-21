-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create Unity Catalog external locations

-- COMMAND ----------

-- Landing

CREATE EXTERNAL LOCATION IF NOT EXISTS extloc_creditrisk_landing_dev_uks_001
URL 'abfss://landing@stcrlandingdevuks001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_creditrisk_dev_uks_001)
COMMENT 'DEV landing external location for raw Home Credit CSV files';

-- COMMAND ----------

--- Bronze
CREATE EXTERNAL LOCATION IF NOT EXISTS extloc_creditrisk_bronze_dev_uks_001
URL 'abfss://bronze@stcrcurateddevuks001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_creditrisk_dev_uks_001)
COMMENT 'DEV bronze external location for raw Delta tables';

-- COMMAND ----------

---Silver
CREATE EXTERNAL LOCATION IF NOT EXISTS extloc_creditrisk_silver_dev_uks_001
URL 'abfss://silver@stcrcurateddevuks001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_creditrisk_dev_uks_001)
COMMENT 'DEV silver external location for cleaned and conformed Delta tables';

-- COMMAND ----------

---Gold
CREATE EXTERNAL LOCATION IF NOT EXISTS extloc_creditrisk_gold_dev_uks_001
URL 'abfss://gold@stcrgolddevuks001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_creditrisk_dev_uks_001)
COMMENT 'DEV gold external location for business-ready marts, feature tables, and dashboard outputs';

-- COMMAND ----------

---Checkpoints
CREATE EXTERNAL LOCATION IF NOT EXISTS extloc_creditrisk_checkpoints_dev_uks_001
URL 'abfss://checkpoints@stcrcurateddevuks001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_creditrisk_dev_uks_001)
COMMENT 'DEV checkpoint location for Auto Loader and structured streaming checkpoints';

-- COMMAND ----------

---Metadata
CREATE EXTERNAL LOCATION IF NOT EXISTS extloc_creditrisk_metadata_dev_uks_001
URL 'abfss://metadata@stcrcurateddevuks001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_creditrisk_dev_uks_001)
COMMENT 'DEV metadata external location for configuration and control tables';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS extloc_creditrisk_logs_dev_uks_001
URL 'abfss://logs@stcrcurateddevuks001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_creditrisk_dev_uks_001)
COMMENT 'DEV logs';

-- COMMAND ----------

--- project/catalog-specific path for uc-managed
CREATE EXTERNAL LOCATION IF NOT EXISTS extloc_creditrisk_uc_managed_dev_uks_001
URL 'abfss://uc-managed@stcrcurateddevuks001.dfs.core.windows.net/credit_risk_dev/'
WITH (STORAGE CREDENTIAL sc_creditrisk_dev_uks_001)
COMMENT 'DEV Unity Catalog managed storage root';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS extloc_creditrisk_powerbi_dev_uks_001
URL 'abfss://powerbi@stcrgolddevuks001.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL sc_creditrisk_dev_uks_001)
COMMENT 'DEV Power BI export and semantic layer output location';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Validations
-- MAGIC
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION extloc_creditrisk_landing_dev_uks_001;


-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION extloc_creditrisk_bronze_dev_uks_001;


-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION extloc_creditrisk_silver_dev_uks_001;
DESCRIBE EXTERNAL LOCATION extloc_creditrisk_gold_dev_uks_001;
DESCRIBE EXTERNAL LOCATION extloc_creditrisk_checkpoints_dev_uks_001;
DESCRIBE EXTERNAL LOCATION extloc_creditrisk_metadata_dev_uks_001;
DESCRIBE EXTERNAL LOCATION extloc_creditrisk_logs_dev_uks_001;
DESCRIBE EXTERNAL LOCATION extloc_creditrisk_powerbi_dev_uks_001;
