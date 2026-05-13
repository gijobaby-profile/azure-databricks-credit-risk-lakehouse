-- Databricks notebook source
-- MAGIC %md
-- MAGIC  File Name   : sql/00_admin/00_create_storage_credential.sql   
-- MAGIC  Project     : Azure Databricks Credit Risk Lakehouse     
-- MAGIC  Purpose     : Create Unity Catalog Storage Credential using Azure Databricks Access Connector Managed Identity   

-- COMMAND ----------

CREATE STORAGE CREDENTIAL IF NOT EXISTS sc_creditrisk_dev_uks_001
WITH AZURE_MANAGED_IDENTITY (
  ACCESS CONNECTOR ID
  '/subscriptions/058477cd-b04e-4fe0-afc1-05f6f4b0ef2c/resourceGroups/rg-creditrisk-dev-uks-001/providers/Microsoft.Databricks/accessConnectors/acadb-creditrisk-dev-uks-001'
)
COMMENT 'DEV storage credential for Credit Risk Lakehouse using Azure Databricks Access Connector managed identity';

-- COMMAND ----------

-- validation command
DESCRIBE STORAGE CREDENTIAL sc_creditrisk_dev_uks_001;

-- COMMAND ----------

--Grant permissions to group created inside Azure Entra ID
GRANT READ FILES, WRITE FILES
ON STORAGE CREDENTIAL sc_creditrisk_dev_uks_001
TO `grp-creditrisk-data-engineers-dev`;
