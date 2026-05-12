# Databricks Jobs

This document lists the Databricks Jobs used by the project and how they are connected to the local Airflow setup.

Databricks is used to run the Spark/lakehouse workloads. Airflow is used as the external orchestrator and triggers Databricks jobs through the Databricks Jobs API.

---

## Current status

At this stage, the project has a working Databricks smoke-test job.

The smoke test confirms that:

- Databricks can run code from this repository;
- Airflow can trigger a Databricks Job;
- the Airflow Databricks connection works;
- the Databricks job can complete successfully;
- test Delta tables can be written in the development catalog.

The full production-style layer jobs are still planned.

---

## Job source

The Databricks job should use Git as the source.

Repository:

```text
https://github.com/Vladcozma90/Stripe-BillingLakehouse
```

Branch:

```text
main
```

Using Git as the source is preferred over manually uploading files to the Databricks workspace because the job runs version-controlled code.

---

## Current Databricks job

### `billinglakehouse_smoke_test`

Purpose:

Runs a small validation job to confirm that the Databricks runtime and the Airflow to Databricks integration work.

This job is not intended to test the full business pipeline. It is a runtime/orchestration check.

Current status:

```text
Passed
```

Expected tasks:

1. Bootstrap smoke test
2. E2E smoke test

Example parameters:

```text
--catalog billinglakehouse_dev
--schema smoke_tests
--log-level INFO
```

Expected output tables:

```text
billinglakehouse_dev.smoke_tests.bootstrap_smoke_test
billinglakehouse_dev.smoke_tests.e2e_gold_revenue_by_country
```

Evidence stored in the repository:

```text
docs/screenshots/airflow_smoke_dag_success.png
docs/screenshots/airflow_databricks_task_log.png
docs/screenshots/databricks_smoke_job_success.png
docs/screenshots/databricks_smoke_tables.png
```

---

## Airflow integration

Airflow triggers the smoke-test job using:

```python
DatabricksRunNowOperator
```

Airflow connection ID:

```text
databricks_default
```

Environment variable used for the Databricks job ID:

```text
DATABRICKS_SMOKE_TEST_JOB_ID
```

The validated flow is:

```text
Airflow DAG
  -> DatabricksRunNowOperator
  -> Databricks Jobs API
  -> Databricks job run
  -> Delta table output
```

The Airflow task log confirms that the Databricks run was submitted, monitored, and completed successfully.

---

## Planned Databricks jobs

The next Databricks jobs will follow the lakehouse layers.

Planned jobs:

```text
billinglakehouse_bootstrap
billinglakehouse_bronze
billinglakehouse_silver
billinglakehouse_gold
```

---

## `billinglakehouse_bootstrap`

Purpose:

Creates the required project objects before the data pipeline runs.

Expected responsibilities:

- create required Unity Catalog schemas;
- create ops tables;
- create or validate required base tables;
- prepare the environment for bronze/silver/gold jobs.

Expected ops tables:

```text
ops.run_logs
ops.pipeline_state
```

This job should be run manually or only when environment setup is required. It should not be part of every scheduled pipeline run unless there is a clear reason.

---

## `billinglakehouse_bronze`

Purpose:

Loads raw source data into Bronze Delta tables.

Expected sources:

```text
Stripe landing JSON
ERP landing Parquet
```

Expected responsibilities:

- read source/landing data;
- preserve raw fields where useful;
- add ingestion metadata;
- write Bronze Delta tables.

Expected metadata columns may include:

```text
_ingest_ts
_ingest_date
_file_name
_source
_landing_format
etl_run_id
```

The Bronze layer should stay close to the source data and avoid heavy business transformations.

---

## `billinglakehouse_silver`

Purpose:

Cleans, validates, deduplicates, quarantines, and conforms Bronze data.

Expected responsibilities:

- read incremental Bronze data;
- apply data quality rules;
- write DQ results;
- quarantine invalid records;
- deduplicate records where needed;
- build current/conformed Silver outputs;
- update run logs and watermarks.

Expected support tables:

```text
silver DQ tables
silver quarantine tables
silver conform/current tables
ops.run_logs
ops.pipeline_state
```

The Silver layer is where most data quality and standardization logic should live.

---

## `billinglakehouse_gold`

Purpose:

Builds business-facing tables for analytics and reporting.

Expected responsibilities:

- read conformed Silver data;
- build dimensions;
- build fact tables;
- expose reporting-ready outputs.

Expected outputs:

```text
Gold dimension tables
Gold fact tables
Power BI-ready tables
```

The Gold layer should contain tables with clear business meaning and stable grains.

---

## Job execution order

Expected execution order:

```text
1. billinglakehouse_bootstrap
2. billinglakehouse_bronze
3. billinglakehouse_silver
4. billinglakehouse_gold
```

For regular scheduled runs, the bootstrap job may be excluded if the environment is already prepared.

A normal scheduled flow is expected to be:

```text
bronze -> silver -> gold
```