# Azure Databricks Credit Risk Lakehouse

A production-inspired Azure Databricks Lakehouse project for credit-risk analytics, built using Medallion Architecture, Delta Lake, PySpark, Spark SQL, metadata-driven ingestion, configurable data quality validation, audit logging, and reusable pipeline components.

This project demonstrates how raw credit-risk datasets can be ingested, standardized, conformed, validated, and prepared for downstream analytics in a scalable banking-style data engineering environment.

## Recruiter Summary

This project demonstrates current hands-on Azure Databricks data engineering experience through a production-inspired credit-risk lakehouse. It covers Bronze ingestion, Silver standardization, Silver conformance, configurable data quality validation, rejected-record handling, audit logging, Azure Data Factory orchestration assets, a CI/CD-ready repository structure, and reusable PySpark modules.

The project is designed to show practical capability in Azure Databricks, PySpark, Spark SQL, Delta Lake, Azure Data Factory, GitHub-based development, and banking/credit-risk data modelling.

---

## 1. Project Overview

This repository implements an end-to-end credit risk data engineering framework on Azure Databricks. The design follows a layered Lakehouse architecture where data is progressively improved from raw ingestion to standardized, conformed, and analytics-ready structures.

The project is built around the Home Credit risk dataset and is designed to simulate real-world banking data engineering patterns, including:

- Metadata-driven ingestion and transformation
- Bronze, Silver Standardized, Silver Conformed, and Gold-oriented design
- Delta Lake managed tables
- Config-driven schema enforcement
- Data quality checks and rejected-record handling
- Audit and error logging
- Reusable PySpark modules
- Databricks notebook orchestration
- GitHub-based project structure suitable for CI/CD extension

The repository is intended to demonstrate practical data engineering capability across Databricks, PySpark, SQL, Delta Lake, and financial-domain data modelling.


## 2. Business Context

Credit risk platforms depend on reliable, traceable, and well-conformed data pipelines. Source files often contain different structures, inconsistent datatypes, missing values, duplicate records, and business-rule violations.

This project addresses those challenges by building a Lakehouse pipeline that:

1. Ingests raw source files into Bronze tables.
2. Standardizes schema, datatypes, and technical metadata in Silver Standardized tables.
3. Builds business-aligned Silver Conformed entities such as customer, loan application, bureau credit, previous application, credit card balance, installment payment, and POS cash balance.
4. Applies metadata-driven data quality rules and captures rejected records.
5. Maintains pipeline auditability through run-level and table-level logging.
6. Prepares clean, trusted datasets for future Gold-layer reporting, feature engineering, and credit risk analytics.


## 3. Target Architecture

```text
Source Files
    |
    v
Bronze Layer
    - Raw persisted Delta tables
    - Source file metadata
    - Ingestion timestamp and ingestion date
    |
    v
Silver Standardized Layer
    - Schema enforcement
    - Column standardization
    - Datatype casting
    - Required-field validation
    - Rejected record capture
    |
    v
Silver Conformed Layer
    - Business entity modelling
    - Customer SCD Type 2
    - business_dt-based batch replacement
    - Derived business attributes
    - Business data quality rules
    |
    v
Gold Layer
    - Analytics-ready tables
    - Credit risk KPIs
    - BI/reporting and ML feature extension
```


## 4. Core Design Principles

### Metadata-Driven Processing

Pipeline behavior is controlled through configuration tables instead of hardcoding table-specific logic inside notebooks.

Metadata controls:

- Source table or source query
- Target table
- Business keys
- Hash columns
- Load strategy
- Derived column rules
- Data quality rules
- Execution sequence

This improves maintainability and allows new entities to be added with minimal code changes.

### Modular PySpark Code

Reusable Python modules are used for common logic such as configuration reading, schema transformation, hash generation, SCD Type 2 handling, data quality validation, Delta write patterns, audit logging, and error logging.

Notebook code is kept lightweight and orchestration-focused.

### Audit-Friendly Batch Design

Each pipeline execution is traceable using:

- `pipeline_run_id`
- `business_dt`
- `created_timestamp`
- `created_date`
- Source file metadata
- Data quality result records
- Rejected records
- Error logs

This design supports controlled reruns and transparent reconciliation.

### Scalable Write Strategy

The Silver Conformed layer uses two loading patterns:

| Entity Type | Load Strategy |
|---|---|
| Customer entity | SCD Type 2 merge |
| Large non-SCD2 entities | `replaceWhere` by `business_dt` |

For high-volume daily snapshot-style entities, replacing only the relevant `business_dt` partition is more efficient and easier to rerun than a full-table overwrite or unnecessary row-level merge.

---

## 5. Technology Stack

| Area | Technology |
|---|---|
| Cloud Data Platform | Azure Databricks |
| Processing Engine | Apache Spark / PySpark |
| Storage Format | Delta Lake |
| Query Language | Spark SQL |
| Architecture Pattern | Medallion Architecture |
| Data Quality | Metadata-driven validation rules |
| Orchestration | Azure Data Factory |
| Version Control | GitHub |
| CI/CD Extension | GitHub Actions-ready structure |
| Domain | Credit Risk / Banking Data Engineering |



## 7. Data Layers

### 7.1 Bronze Layer

The Bronze layer stores raw data in Delta format with minimal transformation. It preserves source-level traceability and technical ingestion metadata.

Typical metadata columns include:

- `source_file_name`
- `source_file_path`
- `pipeline_run_id`
- `ingestion_timestamp`
- `ingestion_date`

Purpose:

- Persist raw input data
- Maintain source traceability
- Support reload and reconciliation
- Provide stable input for standardization

### 7.2 Silver Standardized Layer

The Silver Standardized layer applies technical standardization.

Key responsibilities:

- Column renaming based on metadata
- Datatype casting
- Required-column validation
- Duplicate-key handling
- Technical metadata enrichment
- Rejected-record capture for schema and casting failures

This layer creates source-aligned standardized tables such as:

```text
standardized_application_train
standardized_application_test
standardized_bureau
standardized_previous_application
standardized_credit_card_balance
standardized_installments_payments
standardized_pos_cash_balance
```

### 7.3 Silver Conformed Layer

The Silver Conformed layer creates business-aligned entities from standardized source tables.

Implemented conformed entities:

```text
conformed_customer_scd2
conformed_loan_application
conformed_bureau_credit
conformed_previous_application
conformed_credit_card_balance
conformed_installment_payment
conformed_pos_cash_balance
```

Key features:

- Business-friendly column naming
- `business_dt` for snapshot/batch traceability
- Customer SCD Type 2 history tracking
- Derived metrics and business flags
- Metadata-driven business validations
- `replaceWhere` by `business_dt` for large non-SCD2 tables

---

## 8. Silver Conformance Load Strategy

### Customer SCD Type 2

The customer entity uses SCD Type 2 logic to track historical changes in customer attributes.

SCD2 fields include:

```text
record_hash
effective_from
effective_to
is_current
business_dt
pipeline_run_id
```

The SCD2 process:

1. Builds the current source customer view.
2. Calculates record hash using tracked attributes.
3. Compares source records with current active target records.
4. Expires changed current records.
5. Inserts new versions for new or changed customers.

### Non-SCD2 Entities

Large transactional or snapshot-style conformed entities use:

```python
.option("replaceWhere", f"business_dt = '{business_dt}'")
```

This allows controlled replacement of a single business date partition.

This pattern is suitable when each run contains the complete dataset for that `business_dt`.

---

## 9. Metadata Configuration

### Entity Configuration

The entity configuration table controls table-level behavior.

Example controls:

- `entity_name`
- `target_catalog_name`
- `target_schema_name`
- `target_table_name`
- `source_query`
- `load_strategy`
- `business_key_columns`
- `hash_columns`
- `effective_timestamp_column`
- `is_scd2`
- `load_sequence`

### Derived Column Configuration

The derived column configuration table controls calculated fields.

Example derived fields:

- `age_years`
- `employment_years`
- `credit_income_ratio`
- `annuity_income_ratio`
- `is_active_credit`
- `has_overdue`
- `payment_delay_days`
- `is_late_payment`
- `remaining_installment_ratio`

This makes business transformations transparent and easier to maintain.

---

## 10. Data Quality Framework

The project uses a metadata-driven data quality framework.

Supported rule patterns include:

- Not-null checks
- Domain checks
- Range checks
- Duplicate checks
- SCD2 consistency checks
- Custom SQL-based business rules

Failed records are written to rejected-record structures with:

- Rule identifier
- Rejection reason
- Rejected record JSON
- Source file metadata
- Pipeline run identifier
- Created timestamp

This allows failed records to be investigated without losing the original data context.

---

## 11. Audit and Error Logging

The project includes structured audit logging for:

- Pipeline start and end
- Table load start and end
- Record counts
- Success and failure status
- Error messages
- Pipeline run identifiers

This supports operational monitoring and makes the pipeline easier to debug and reconcile.

---

## 12. Execution Flow

A typical execution sequence is:

```text
1. Create catalog, schemas, volumes, and external locations
2. Create configuration and audit tables
3. Insert ingestion and transformation metadata
4. Create Bronze tables
5. Load source files into Bronze
6. Create Silver Standardized tables
7. Run Silver Standardization pipeline
8. Create Silver Conformed tables
9. Insert Silver Conformance metadata
10. Run Silver Conformance pipeline
11. Validate DQ results and rejected records
12. Run maintenance commands where required
```



## 13. How to Run

### Step 1: Create Base Objects

Run the administrative SQL scripts under:

```text
sql/00_admin/
```

### Step 2: Create Metadata and Audit Tables

Run scripts under:

```text
sql/01_metadata/
```

### Step 3: Create Lakehouse Tables

Run table DDLs under:

```text
sql/03_tables/
```

### Step 4: Insert Metadata

Insert configuration for:

- Bronze ingestion
- Silver column standardization
- Silver Conformance entities
- Derived columns
- Data quality rules

For long SQL metadata such as `source_query`, Python-based metadata insertion is preferred to avoid SQL string escaping issues.

### Step 5: Run Notebooks

Run the Databricks notebooks in sequence:

```text
Bronze ingestion notebook
Silver Standardization notebook
Silver Conformance notebook
Gold/analytics notebook
```

For the Silver Conformance layer:

```text
notebooks/03_silver_conformance/
├── 01_build_customer_scd2.py
├── 02_build_conformed_entity.py
└── 99_run_all_silver_conformance.py
```

---

## 14. Key Engineering Highlights

- Designed a metadata-driven Medallion Architecture on Azure Databricks
- Built reusable PySpark modules instead of hardcoded notebook-only logic
- Implemented schema enforcement and standardized column mapping
- Added data quality validation and rejected-record handling
- Implemented SCD Type 2 for customer history tracking
- Used `business_dt` for audit-friendly batch processing
- Applied partition-level replacement using Delta `replaceWhere`
- Added record hashing for change detection
- Implemented structured audit and error logging
- Prepared the architecture for CI/CD and workflow orchestration

---

## 15. Implementation Status 

### 15.1. Phase 1 Status

This project is implemented as a production-inspired Azure Databricks credit-risk lakehouse. The core Databricks lakehouse implementation is completed, while orchestration, deployment automation, and selected extension features are being added in controlled phases.

| Area | Component | Status | Comments |
|---|---|---|---|
| Databricks Lakehouse | Bronze ingestion | Completed | Raw source files are loaded into Bronze Delta tables with source metadata and pipeline tracking. |
| Databricks Lakehouse | Silver Standardization | Completed | Standardized tables are created using metadata-driven column mapping, datatype casting, validation logic, and rejected-record handling. |
| Databricks Lakehouse | Silver Conformance | Completed | Business-aligned conformed entities are created from standardized source tables. |
| Databricks Lakehouse | Customer SCD Type 2 | Completed | Customer history is tracked using hash-based change detection and effective-date handling. |
| Databricks Lakehouse | Data Quality Framework | Completed | Metadata-driven data quality rules are applied with rejected-record handling and rule-level traceability. |
| Databricks Lakehouse | Audit and Error Logging | Completed | Pipeline execution, table loads, record counts, and errors are captured for operational traceability. |
| Databricks Lakehouse | Gold Risk Marts | Completed | Analytics-ready Gold-layer risk marts are created for credit-risk KPI analysis. |
| Databricks Lakehouse | Reusable PySpark Modules | Completed | Common logic is modularized for configuration reading, transformations, DQ validation, SCD handling, and logging. |
| Databricks Asset Bundles | `databricks.yml` | Completed | Root bundle configuration is available for Databricks Asset Bundle structure. |
| Databricks Asset Bundles | Resource Definitions | In Progress | Resource files are being prepared for jobs, workflows, parameters, and environment-specific deployment settings. |
| CI/CD | GitHub Repository Structure | Completed | Repository is organized with notebooks, source modules, SQL scripts, ADF assets, resources, and documentation. |
| CI/CD | GitHub Actions Workflow Structure | In Progress | Workflow files are being prepared for validation and deployment automation. |
| Azure Data Factory | Linked Services | Completed | Linked services are configured for ADLS, Azure Databricks, and Key Vault integration. |
| Azure Data Factory | Datasets | Completed | ADF datasets are being configured for source, control, and orchestration metadata access. |
| Azure Data Factory | Test Pipeline | Completed | A validation pipeline will confirm linked services, parameters, and connectivity. |
| Azure Data Factory | Databricks Test Execution | Completed | ADF will trigger a Databricks notebook/job to validate execution from ADF to Databricks. |
| Azure Data Factory | File Watcher Pipeline | Completed | A file-watcher pipeline will check landing-zone file availability before downstream processing starts. |
| Azure Data Factory | Main Orchestration Pipeline | Completed | The main orchestration pipeline will coordinate Bronze, Silver, Conformed, and Gold execution using dependency checks and business date parameters. |


### 15.2. Phase 2 Extensions

| Area | Component | Status | Comments |
|---|---|---|---|
| Phase 2 Extension | Power BI Dashboard | Planned | Build dashboard views for Gold-layer credit risk KPIs and analytical outputs. |
| Phase 2 Extension | dbt Gold-Layer Modelling | Planned | Add modular SQL modelling, testing, documentation, and lineage support for Gold-layer transformations. |
| Phase 2 Extension | Configurable Debug Mode | Planned | Add development-time debug logging while keeping production logs clean and readable. |
| Phase 2 Extension | Incremental Load Support | Planned | Extend selected source entities with partial or change-based processing where business requirements justify it. |
| Phase 2 Extension | Streaming Source Integration | Planned | Add streaming or near-real-time ingestion to demonstrate Databricks streaming capabilities. |

---

## 16. Repository Structure

```text
.github/workflows/   - GitHub Actions CI/CD workflows
adf/                 - Azure Data Factory pipelines, datasets, linked services and triggers
config/              - Environment and project-level configuration
docs/                - Architecture and implementation documentation
notebooks/           - Databricks notebooks organized by lakehouse layer
resources/           - Databricks Asset Bundle resource definitions
sql/                 - DDL, metadata inserts, audit and orchestration SQL scripts
src/                 - Reusable Python/PySpark modules
databricks.yml       - Databricks Asset Bundle root configuration
```
---

## 17. Business Value Demonstrated

This project simulates how a financial institution can build controlled and traceable data pipelines for credit-risk analytics. It demonstrates:
- Improved data traceability from source files to conformed business entities.
- Reduced hardcoding through metadata-driven ingestion and transformation.
- Better data reliability through configurable data quality validation.
- Support for controlled reruns using pipeline_run_id and business_dt.
- Audit-friendly processing suitable for regulated banking environments.
- Preparation of trusted datasets for reporting, BI, and credit-risk feature engineering.

---

## 18. Author

**Gijo Baby**  
Senior Data Engineer  
Azure Databricks | PySpark | Data Warehousing | Credit Risk | Banking Data Engineering

GitHub: [gijobaby-profile](https://github.com/gijobaby-profile)
