Paste this entire block into `docs/ARCHITECTURE.md`.

````markdown
# Architecture

The project is designed as a small but realistic lakehouse pipeline using Stripe API data, ERP-style reference and usage data, Databricks for Spark processing and Delta tables, Airflow for orchestration, YAML-based configuration, and ops tables for logging and incremental state.

The goal is to show an end-to-end data engineering workflow, not only isolated transformation scripts.

---

## High-level flow

```text
Stripe API / ERP files
        |
        v
Landing / Raw source files
        |
        v
Bronze Delta tables
        |
        v
Silver validation, cleaning, deduplication, quarantine
        |
        v
Gold dimensional model
        |
        v
BI / reporting layer
````

Airflow is used to orchestrate Databricks jobs. Databricks is responsible for the Spark/lakehouse workloads.

---

## Main components

### Source systems

The project uses two source categories.

#### Stripe API

Stripe provides billing-related data such as:

* customers;
* subscriptions;
* subscription items;
* invoices.

The API extraction layer is responsible for pulling data from Stripe and storing it in a landing/raw format before lakehouse processing.

#### ERP-style files

The ERP-style datasets represent internal business data used to enrich and analyze Stripe billing data.

Examples:

* account master snapshot;
* plan catalog;
* usage daily.

These sources are expected to arrive as files, such as Parquet files, in a landing/raw storage location.

---

## Orchestration layer

Airflow is used as the external orchestrator.

In the local setup, Airflow runs in Docker and triggers Databricks Jobs through the Databricks Jobs API.

Current validated orchestration path:

```text
Airflow DAG
  -> DatabricksRunNowOperator
  -> Databricks Jobs API
  -> Databricks Job
  -> Delta table output
```

The Databricks connection used by Airflow is:

```text
databricks_default
```

The smoke-test Databricks job ID is passed through:

```text
DATABRICKS_SMOKE_TEST_JOB_ID
```

The same pattern is planned for the real pipeline jobs.

---

## Databricks layer

Databricks is used to run Spark jobs and write Delta tables.

The planned Databricks jobs are:

```text
billinglakehouse_bootstrap
billinglakehouse_bronze
billinglakehouse_silver
billinglakehouse_gold
```

The smoke-test job has already been validated and is used as a runtime/orchestration check.

The real jobs should be created gradually and tested one layer at a time.

---

## Storage layout

The project separates data by lakehouse layer.

Expected logical layers:

```text
landing
bronze
silver
gold
ops
```

Typical configured base paths:

```text
landing_base_path
bronze_base_path
silver_base_path
gold_base_path
ops_base_path
checkpoint_base_path
```

The exact values are environment-specific and are loaded from YAML configuration.

---

## Catalog and schemas

The project uses separate schemas for each layer.

Expected schema structure:

```text
<catalog>.<bronze_schema>
<catalog>.<silver_schema>
<catalog>.<gold_schema>
<catalog>.<ops_schema>
```

Example development layout:

```text
billinglakehouse_dev.bronze
billinglakehouse_dev.silver
billinglakehouse_dev.gold
billinglakehouse_dev.ops
```

The exact schema names are controlled by the environment configuration.

---

## Configuration

Project configuration is loaded from YAML files.

Example location:

```text
configs/dev.yaml
```

Configuration includes:

* catalog name;
* schema names;
* storage paths;
* dataset definitions;
* source settings;
* data quality rules.

The purpose of the YAML configuration is to keep environment-specific settings outside the pipeline logic.

Secrets should not be stored in YAML files. API tokens and credentials should be stored in a secret manager or provided through secure runtime configuration.

---

## Bronze layer

The Bronze layer stores raw or lightly standardized data.

Responsibilities:

* read source/landing data;
* preserve source fields where useful;
* add technical ingestion metadata;
* write Delta tables;
* provide a stable input for Silver processing.

Typical metadata columns may include:

```text
_ingest_ts
_ingest_date
_file_name
_source
_landing_format
etl_run_id
```

The Bronze layer should avoid heavy business logic. Its main purpose is to preserve source data in a queryable Delta format.

---

## Silver layer

The Silver layer is responsible for data cleaning, validation, deduplication, quarantine, and conformed outputs.

Responsibilities:

* read incremental Bronze data;
* validate required columns;
* apply dataset-specific DQ rules;
* write DQ results;
* quarantine invalid records;
* deduplicate records by business key;
* build current or conformed Silver tables;
* update watermarks and run logs.

The Silver layer is where most standardization logic belongs.

Examples of Silver outputs:

```text
s_dq_<dataset>
s_quarantine_<dataset>
s_current_<dataset>
s_conform_<dataset>
```

Not every dataset needs every table type. The pattern depends on the dataset and business use case.

---

## Data quality

Data quality rules are defined in configuration and executed by the DQ service.

Supported rule types include:

```text
max_null
min_value
accepted_values
unique
valid_date
valid_timestamp
```

The DQ service returns a normalized metrics structure containing:

* total rows;
* rule-level results;
* failed rows;
* actual values;
* thresholds;
* overall result.

DQ results can be written to Silver DQ tables for auditability.

Invalid business-key records can be written to quarantine tables.

The DQ logic has been unit-tested to ensure malformed dates and timestamps are counted as DQ failures instead of crashing Spark execution.

---

## Quarantine pattern

Records with invalid or missing business keys should not be loaded into conformed outputs.

The quarantine pattern separates records into:

```text
good_records
bad_records
```

Bad records are written to quarantine tables with a quarantine reason and timestamp.

This makes failures visible without silently dropping records.

---

## Incremental processing

The project uses watermark-based incremental processing.

The expected pattern is:

1. Read the last processed watermark from the ops state table.
2. Filter source data using an incremental timestamp column.
3. Process only new records.
4. Write output tables.
5. Update the watermark after successful processing.

The state table is expected to track values such as:

```text
pipeline_name
dataset
last_watermark_ts
updated_at
updated_by_run_id
```

This pattern avoids reprocessing the full dataset on every run.

---

## Ops tables

The ops layer stores operational metadata.

Expected ops tables:

```text
ops.run_logs
ops.pipeline_state
```

### `ops.run_logs`

Tracks pipeline execution metadata.

Typical fields:

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

### `ops.pipeline_state`

Tracks incremental state.

Typical fields:

```text
pipeline_name
dataset
last_watermark_ts
updated_at
updated_by_run_id
```

The ops layer is important for observability, troubleshooting, and incremental processing.

---

## Gold layer

The Gold layer contains business-facing tables.

Responsibilities:

* read conformed Silver data;
* build dimensions;
* build fact tables;
* expose stable reporting outputs.

Expected Gold model types:

```text
dimension tables
fact tables
```

Examples may include:

```text
dim_customers
dim_accounts
dim_plan_catalog
fact_invoices
fact_usage_daily
fact_billing_revenue
```

The Gold layer should be designed around clear business grains and reporting use cases.

---

## Reporting layer

The Gold layer is intended to be consumed by BI tools such as Power BI.

Power BI should connect to curated Gold tables rather than directly to Bronze or raw Silver tables.

Expected reporting pattern:

```text
Gold Delta tables
  -> Databricks SQL / warehouse
  -> Power BI
```

This keeps reporting isolated from raw ingestion and intermediate processing details.

---

## Current validated state

The following items have already been validated:

```text
Unit tests: passed
Airflow to Databricks smoke orchestration: passed
Databricks smoke job execution: passed
Smoke-test Delta outputs: passed
```

The current unit test result is:

```text
11 passed
```

The smoke-test job validates that Airflow can trigger Databricks and that Databricks can execute project code.

---

## Current limitations

The architecture is not yet fully validated against live ADLS-backed data.

Current limitations:

* full real bronze/silver/gold jobs still need to be created and tested;
* live ADLS integration still needs validation;
* real Stripe API extraction needs final integration testing;
* full reconciliation between source and Gold outputs is not yet complete;
* performance testing is not yet in scope.

These are expected next-stage tasks.

---

## Planned execution flow

The planned full pipeline order is:

```text
bootstrap
  -> bronze
  -> silver
  -> gold
```

For recurring scheduled runs, bootstrap may be excluded once the environment is already prepared.

Normal recurring flow:

```text
bronze -> silver -> gold
```