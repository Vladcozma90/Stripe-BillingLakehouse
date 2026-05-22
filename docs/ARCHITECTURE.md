````markdown
# Architecture

## Overview

`Stripe-BillingLakehouse` is a portfolio data engineering project that implements a realistic lakehouse pipeline for billing analytics.

The project combines Stripe billing data with ERP-style business data and processes it through a medallion architecture:

```text
Source systems
    |
    v
Landing / raw files
    |
    v
Bronze Delta tables
    |
    v
Silver validation, cleaning, deduplication, quarantine, conformance
    |
    v
Gold dimensional model
    |
    v
BI / reporting layer
````

The main objective is to demonstrate production-style data engineering patterns:

```text
orchestration
layered lakehouse design
data quality handling
incremental processing
audit logging
SCD2 conformance
Gold dimensional modeling
clear separation between technical layers and BI-facing outputs
```

The project is intentionally portfolio-scale, but the design follows patterns commonly used in production lakehouse environments.

---

## Technology stack

| Area                        | Technology                                  |
| --------------------------- | ------------------------------------------- |
| Orchestration               | Apache Airflow                              |
| Compute                     | Databricks / Apache Spark                   |
| Storage format              | Delta Lake                                  |
| Programming language        | Python / PySpark                            |
| Configuration               | YAML + environment variables                |
| Secrets                     | Azure Key Vault pattern                     |
| Metadata and audit          | Delta ops tables                            |
| Local orchestration runtime | Docker / Docker Compose                     |
| Reporting target            | Power BI or any BI tool reading Gold tables |

Airflow is responsible for orchestration.

Databricks is responsible for Spark-based lakehouse processing.

Delta Lake is used as the storage format for Bronze, Silver, Gold, and Ops tables.

---

## Source systems

The project uses two source categories.

### Stripe API sources

Stripe provides billing-related operational data:

```text
stripe_customers
stripe_subscriptions
stripe_subscription_items
stripe_invoices
```

These datasets represent customers, subscriptions, subscription line items, and invoices.

### ERP-style sources

ERP-style data provides internal business context used to enrich the Stripe billing data:

```text
erp_account_master_snapshot
erp_plan_catalog
erp_usage_daily
```

These datasets represent account metadata, plan/product metadata, and daily usage metrics.

---

## Repository structure

The repository is organized around platform responsibilities:

```text
Stripe-BillingLakehouse/
|
├── bootstrap/              # Environment and table bootstrap logic
├── configs/                # Environment-specific YAML configuration
├── dags/                   # Airflow DAG definitions
├── docs/                   # Project documentation
├── jobs/                   # Job entrypoints
│   ├── bootstrap/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── src/
│   ├── connectors/         # Source/API connectors
│   ├── pipelines/          # Bronze, Silver, Gold pipeline logic
│   └── services/           # Shared services: env loading, audit, logging, secrets
└── tests/                  # Unit and smoke tests
```

The design separates job entrypoints from reusable pipeline logic.

Files under `jobs/` should remain thin wrappers. Core transformation logic belongs under `src/pipelines/`.

---

## Environment and configuration model

Runtime behavior is controlled through YAML configuration and environment variables.

YAML configuration defines non-secret project settings such as:

```text
catalog
schemas
storage paths
source dataset configuration
data quality rules
pipeline-level settings
```

Environment variables define runtime execution settings such as:

```text
AIRFLOW_UID
ENV
LOG_LEVEL
APP_CONFIG_DIR
DATABRICKS_CONN_ID
DATABRICKS_BOOTSTRAP_JOB_ID
DATABRICKS_BRONZE_JOB_ID
DATABRICKS_SILVER_JOB_ID
DATABRICKS_GOLD_JOB_ID
DATABRICKS_SMOKE_TEST_JOB_ID
```

The intended separation is:

```text
Non-secret project configuration -> YAML
Runtime selectors and job IDs    -> environment variables
Secret values                    -> Azure Key Vault or platform secret manager
```

Secrets should not be hard-coded in the repository.

---

## Configuration loading

The pipeline should resolve its runtime configuration using:

```text
ENV
APP_CONFIG_DIR
```

For example:

```text
ENV=dev
APP_CONFIG_DIR=/opt/airflow/app/configs
```

This should resolve:

```text
/opt/airflow/app/configs/dev.yaml
```

This approach allows the same codebase to run against different environments by changing environment variables and YAML configuration.

---

## Orchestration architecture

Airflow is the external orchestrator.

The standard scheduled flow is:

```text
extract source data to landing
        |
        v
trigger Databricks Bronze job
        |
        v
trigger Databricks Silver job
        |
        v
trigger Databricks Gold job
```

Airflow handles:

```text
scheduling
dependencies
retries
job triggering
basic orchestration visibility
```

Databricks handles:

```text
Spark execution
Delta Lake reads/writes
Bronze ingestion
Silver transformations
Gold model creation
```

This keeps Airflow focused on orchestration and avoids running heavy Spark workloads inside Airflow workers.

---

## Databricks job model

The project is designed around separate Databricks jobs for each major stage:

```text
billinglakehouse_bootstrap
billinglakehouse_bronze
billinglakehouse_silver
billinglakehouse_gold
billinglakehouse_smoke_test
```

The Databricks job IDs are passed to Airflow through environment variables.

Bootstrap and smoke-test jobs are separated from the normal scheduled pipeline.

---

## Bootstrap job

The bootstrap job prepares the lakehouse environment.

Responsibilities:

```text
create or validate schemas
create or validate ops tables
create or validate Bronze tables
create or validate Silver tables
create or validate Gold tables
seed required default/unknown rows where appropriate
```

Expected core schemas:

```text
bronze
silver
gold
ops
```

Expected ops tables:

```text
ops.run_logs
ops.pipeline_state
```

Bootstrap should be safe to rerun and should avoid destructive behavior unless explicitly requested.

---

## Smoke test job

The smoke test job validates that the Databricks workspace can execute a simple job and write/read Delta data.

The smoke test is not part of the production medallion architecture.

It exists to validate:

```text
Databricks job execution
basic Spark startup
basic Delta write/read behavior
environment connectivity
```

Smoke-test objects should be treated as test artifacts, not core Bronze/Silver/Gold/Ops tables.

---

## Bronze job

The Bronze job loads source data into raw Delta tables.

Responsibilities:

```text
read landing files
preserve source-level fields
standardize ingestion metadata
write Bronze Delta tables
record audit information
```

Bronze should stay close to the original source data.

Bronze does not own business conformance, dimensional modeling, or heavy transformation logic.

---

## Silver job

The Silver job validates, cleans, deduplicates, quarantines, and conforms Bronze data.

Responsibilities:

```text
read Bronze tables
apply data quality rules
split valid and invalid records
write DQ result tables
write quarantine tables
deduplicate records
build current outputs
build conformed outputs
maintain SCD2 history where required
update audit logs and pipeline state
```

Silver is the main technical quality and conformance layer.

Silver conform tables preserve business keys, source/natural keys, SCD2 validity columns, record hashes, and audit metadata.

Silver does not act as the BI star-schema layer.

---

## Gold job

The Gold job builds the reporting-ready analytical model.

Responsibilities:

```text
read Silver conform/current tables
combine related Silver entities into business dimensions
build BI-ready dimensions
build BI-ready facts
resolve fact-to-dimension relationships
publish Gold Delta tables
update audit logs
```

Gold is the business-facing consumption layer.

Power BI or another BI tool should consume Gold tables, not Bronze or Silver tables.

Detailed Gold table grains, keys, relationships, and unknown-row handling are documented in `docs/DATA_MODEL.md`.

---

## Medallion layer responsibilities

## Landing layer

The landing layer stores extracted source data before lakehouse processing.

Typical landing data includes:

```text
Stripe API JSON responses
ERP Parquet files
source extracts partitioned by extraction date
```

Landing files are append-oriented and source-preserving.

---

## Bronze layer

Bronze stores raw data in Delta format with technical ingestion metadata.

Typical Bronze metadata columns:

```text
_ingest_ts
_ingest_date
_file_name
_source
_landing_format
etl_run_id
```

Bronze tables should answer:

```text
What data did we receive?
When did we receive it?
Where did it come from?
Which run loaded it?
```

Bronze should preserve replayability. If Silver logic changes, Bronze data should still allow the pipeline to be reprocessed.

---

## Silver layer

Silver owns technical data quality and conformance.

Silver outputs may include:

```text
s_dq_<dataset>
s_quarantine_<dataset>
s_current_<dataset>
s_conform_<dataset>
```

Silver responsibilities:

```text
standardize data types
validate required fields
deduplicate records
quarantine invalid records
maintain current-state records
maintain SCD2 conformed history where required
preserve lineage and audit metadata
```

Silver should rely on business keys, source/natural keys, effective timestamps, record hashes, and audit metadata.

Detailed key design and SCD2 lookup behavior are documented in `docs/DATA_MODEL.md`.

---

## Gold layer

Gold owns the BI-facing analytical model.

Gold outputs include dimensions and facts used for billing and usage analytics.

Expected analytical areas:

```text
customer/account reporting
plan/product reporting
subscription reporting
invoice analysis
subscription item analysis
usage analysis
```

Gold is optimized for business consumption, not for preserving every source-system technical field.

Detailed Gold dimensions, facts, grains, and joins are documented in `docs/DATA_MODEL.md`.

---

## Ops layer

The Ops layer stores metadata required for observability and incremental processing.

Expected ops tables:

```text
ops.run_logs
ops.pipeline_state
```

The Ops layer supports:

```text
pipeline run tracking
success/failure visibility
row count tracking
quarantine count tracking
watermark management
debugging and recovery
```

Ops tables are part of the lakehouse control plane, not part of the business reporting model.

---

## Data quality and quarantine

Data quality checks are applied in Silver.

Typical checks include:

```text
required key presence
valid timestamps
accepted status values
non-negative amounts
valid currencies
deduplication rules
business key uniqueness
```

Invalid records are not silently dropped. They are written to quarantine tables with metadata explaining why the record failed validation.

Typical quarantine metadata includes:

```text
quarantine_reason
quarantine_ts
source identifiers
raw business fields
etl_run_id
```

This design makes bad data auditable and recoverable.

---

## Incremental processing and pipeline state

The project uses ops tables to support incremental processing and observability.

### `ops.pipeline_state`

Stores the last successful processing watermark per pipeline/dataset.

Typical columns:

```text
pipeline_name
dataset
last_watermark_ts
updated_at
updated_by_run_id
```

This table allows jobs to process only new or changed data after the last successful run.

### `ops.run_logs`

Stores run-level audit metadata.

Typical columns:

```text
pipeline_name
dataset
target_table
run_id
started_at
finished_at
status
rows_in
rows_out
rows_quarantined
dq_result
last_watermark_ts
error_msg
```

This table supports debugging, operational monitoring, and portfolio-level demonstration of observability.

---

## Audit and error handling

Each pipeline should follow a consistent audit pattern:

```text
1. Generate run_id.
2. Insert STARTED run log.
3. Read input data.
4. Transform and validate data.
5. Write target tables.
6. Update watermark/state only after successful processing.
7. Mark run SUCCESS.
8. On exception, mark run FAILED and re-raise the error.
```

Watermarks should not be advanced if the target write fails.

This protects the pipeline from marking unprocessed data as completed.

---

## Data lineage

The project maintains lineage through metadata propagation across layers.

Typical lineage metadata includes:

```text
etl_run_id
_ingest_ts
_ingest_date
_file_name
_source
_landing_format
silver_effective_start_ts
silver_effective_end_ts
gold_processed_ts
gold_processed_date
```

Layer-level lineage:

```text
Landing file
    -> Bronze table with ingestion metadata
    -> Silver DQ/current/conform tables
    -> Gold dimensions and facts
    -> BI/reporting
```

The goal is to make each record traceable back to its source extract and pipeline run.

---

## Table naming conventions

Recommended naming pattern:

```text
Bronze:
    b_<source_dataset>

Silver:
    s_dq_<dataset>
    s_quarantine_<dataset>
    s_current_<dataset>
    s_conform_<dataset>

Gold:
    g_dim_<business_entity>
    g_fact_<business_process>

Ops:
    run_logs
    pipeline_state
```

Examples:

```text
b_stripe_customers
s_conform_stripe_customers
s_conform_erp_account_master_snapshot
g_dim_customers
g_fact_invoices
```

Detailed table-level modeling is documented in `docs/DATA_MODEL.md`.

---

## Schema organization

The intended schema separation is:

```text
<catalog>.bronze
<catalog>.silver
<catalog>.gold
<catalog>.ops
```

Each layer has a separate responsibility:

| Schema | Responsibility                                 |
| ------ | ---------------------------------------------- |
| bronze | Raw Delta representation of source data        |
| silver | Cleaned, validated, conformed, historical data |
| gold   | Business-facing dimensional model              |
| ops    | Audit logs and pipeline state                  |

Code should consistently use configured schema names from the environment configuration instead of hard-coded schema strings.

---

## Reporting layer

The Gold layer is the intended reporting interface.

Power BI or another BI tool should consume only Gold tables.

Expected reporting outcomes:

```text
customer billing overview
revenue by customer
revenue by plan
invoice status analysis
subscription analysis
usage by account and plan
```

The reporting layer should not depend directly on Bronze or Silver implementation details.

---

## Testing strategy

The project includes unit and smoke-test coverage.

Testing is expected to validate:

```text
configuration loading
import safety
data quality logic
job execution smoke checks
basic Delta write/read behavior
```

The smoke test validates execution and connectivity, but it is not a replacement for full integration testing.

Detailed testing scope is documented in `docs/TESTING.md`.

---

## Design decisions

### Airflow orchestrates, Databricks computes

Airflow is used for scheduling, dependency management, retries, and triggering Databricks jobs.

Databricks is used for Spark execution and Delta Lake processing.

This avoids running heavy Spark workloads directly inside Airflow workers.

### Bronze remains source-preserving

Bronze keeps data close to the original source format.

This makes the pipeline easier to replay if transformation logic changes.

### Silver owns data quality and conformance

Silver is where records become cleaned, validated, conformed, and auditable.

SCD2 history is maintained in Silver where historical conformance is required.

### Gold owns the BI-facing model

Gold creates the analytical model consumed by BI tools.

Gold hides source-system complexity and exposes dimensions and facts suitable for reporting.

### Hashes are used for change detection

Record hashes are used for deduplication and SCD2 change detection.

Gold dimensional key choices and fact-to-dimension resolution rules are documented in `docs/DATA_MODEL.md`.

---

## Current limitations

This project is designed as a realistic portfolio project, not a complete enterprise platform.

Known limitations:

```text
full production deployment is not included
CI/CD should be expanded
integration tests should be expanded
real production secrets must be configured outside the repo
infrastructure provisioning is not fully automated
observability could be extended with alerting
data volumes are portfolio-scale, not enterprise-scale
```

These limitations are acceptable for a portfolio project if they are documented clearly and the core pipeline design is coherent.

---

## Target outcome

The target outcome is a working lakehouse pipeline that demonstrates:

```text
API ingestion
file ingestion
Bronze/Silver/Gold medallion architecture
data quality and quarantine
SCD2 conformance
audit logging
incremental processing
Airflow-to-Databricks orchestration
BI-ready Gold outputs
```

The project should be understandable to a technical interviewer and defensible as a pragmatic, production-inspired data engineering design.

````

Main changes I made:

```text
Removed from ARCHITECTURE.md:
- detailed surrogate key strategy
- detailed unknown/default row strategy
- detailed Gold dimension construction
- detailed fact construction
- table grain explanations
- detailed fact/dimension relationship logic

Kept in ARCHITECTURE.md:
- high-level Gold responsibility
- high-level Silver responsibility
- system architecture
- orchestration
- config/secrets
- medallion layers
- ops/audit/watermarks
- testing and limitations
````