# Stripe-BillingLakehouse

`Stripe-BillingLakehouse` is a portfolio data engineering project that implements a realistic lakehouse pipeline for billing analytics.

The project combines Stripe billing data with ERP-style business data and processes it through a medallion architecture using Airflow, Databricks, Spark, Delta Lake, and Python.

The goal is to demonstrate practical data engineering patterns used in modern cloud/lakehouse environments:

```text
API ingestion
file ingestion
Bronze / Silver / Gold lakehouse design
data quality and quarantine handling
SCD2 conformance
incremental processing
audit logging
Airflow orchestration
Databricks job execution
Gold dimensional modeling
BI-ready outputs
`````

---

## Business scenario

A company uses Stripe for billing operations and also keeps internal ERP-style data about accounts, plans, and daily usage.

The business needs a clean analytical model that can answer questions such as:

```text
How much revenue was invoiced?
Which customers generated the most billing activity?
Which plans are used most often?
How does usage evolve by customer and plan?
Which invoices are paid, open, void, or overdue?
```

The project builds a lakehouse pipeline that ingests, validates, conforms, and models this data into Gold tables ready for BI/reporting.

---

## Architecture summary

The project follows a medallion architecture:

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
```

Layer responsibilities:

| Layer   | Purpose                                                                           |
| ------- | --------------------------------------------------------------------------------- |
| Landing | Stores extracted source files before lakehouse processing                         |
| Bronze  | Preserves raw source data in Delta format with ingestion metadata                 |
| Silver  | Applies validation, data quality, deduplication, quarantine, and SCD2 conformance |
| Gold    | Publishes BI-ready dimensions and facts                                           |
| Ops     | Stores run logs, pipeline state, and audit metadata                               |

Full architecture documentation:

[Architecture](docs/ARCHITECTURE.md)

---

## Technology stack

| Area             | Technology                                  |
| ---------------- | ------------------------------------------- |
| Orchestration    | Apache Airflow                              |
| Compute          | Databricks / Apache Spark                   |
| Storage format   | Delta Lake                                  |
| Language         | Python / PySpark                            |
| Configuration    | YAML + environment variables                |
| Secrets pattern  | Azure Key Vault                             |
| Local runtime    | Docker / Docker Compose                     |
| Reporting target | Power BI or any BI tool reading Gold tables |

Airflow is used to orchestrate the pipeline and trigger Databricks jobs.

Databricks is used to run the Spark/Delta Lake processing workloads.

---

## Source systems

The project uses two source groups.

### Stripe API data

```text
stripe_customers
stripe_subscriptions
stripe_subscription_items
stripe_invoices
```

These represent billing-platform entities such as customers, subscriptions, subscription items, and invoices.

### ERP-style data

```text
erp_account_master_snapshot
erp_plan_catalog
erp_usage_daily
```

These represent internal account metadata, product/plan metadata, and daily usage records.

---

## Repository structure

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

The project separates job entrypoints from reusable pipeline logic.

`jobs/` contains thin executable wrappers.

`src/pipelines/` contains the core transformation logic.

---

## Pipeline flow

The intended scheduled flow is:

```text
Extract source data to landing
        |
        v
Bronze ingestion
        |
        v
Silver validation and conformance
        |
        v
Gold dimensional modeling
        |
        v
BI/reporting consumption
```

Airflow orchestrates the pipeline and triggers Databricks jobs for the main lakehouse stages.

Expected Databricks job groups:

```text
bootstrap
bronze
silver
gold
smoke_test
```

---

## Data model summary

The Gold layer exposes a dimensional model for billing and usage analytics.

Expected Gold dimensions:

```text
g_dim_customers
g_dim_plan_catalog
g_dim_stripe_subscriptions
```

Expected Gold facts:

```text
g_fact_stripe_invoices
g_fact_stripe_subscription_items
g_fact_usage_daily
```

The model uses Gold-owned surrogate keys for BI-facing joins.

Silver conform tables preserve business keys, source/natural keys, SCD2 validity columns, record hashes, and audit metadata.

Full data model documentation:

[Data Model](docs/DATA_MODEL.md)

---

## Key design decisions

### Airflow orchestrates, Databricks computes

Airflow handles scheduling, dependencies, retries, and job triggering.

Databricks handles Spark execution and Delta Lake processing.

This avoids running heavy Spark workloads directly inside Airflow workers.

### Bronze stays source-preserving

Bronze keeps data close to the source format so the pipeline can be replayed if transformation logic changes.

### Silver owns quality and conformance

Silver applies validation, deduplication, quarantine logic, current-state processing, and SCD2 history where required.

### Gold owns BI modeling

Gold exposes business-facing dimensions and facts.

BI tools should consume Gold tables, not Bronze or Silver implementation tables.

### Gold owns surrogate keys

Silver does not expose dimensional surrogate keys.

Silver uses business keys and SCD2 metadata.

Gold creates BI-facing surrogate keys used by facts and reporting tools.

---

## Configuration and secrets

The project separates configuration from secrets.

```text
YAML config       -> non-secret project settings
Environment vars -> runtime settings and job IDs
Key Vault         -> secrets
```

Example runtime variables:

```text
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

A `.env.example` file should be committed as a safe template.

A real `.env` file should not be committed.

Full setup documentation:

[Setup](docs/SETUP.md)

---

## Data quality and observability

The project includes patterns for:

```text
data validation
quarantine tables
run-level audit logging
row count tracking
watermark/state management
pipeline success/failure tracking
```

Expected ops tables:

```text
ops.run_logs
ops.pipeline_state
```

Invalid records are not silently dropped. They are written to quarantine outputs with failure metadata.

This makes the pipeline easier to debug, audit, and recover.

---

## Testing

The project includes unit and smoke-test coverage.

Testing is intended to validate:

```text
configuration loading
import safety
data quality logic
Databricks smoke execution
basic Delta write/read behavior
```

The smoke test validates connectivity and execution behavior, but it is not a replacement for full production integration testing.

Full testing documentation:

[Testing](docs/TESTING.md)

---

## Documentation

| Document                             | Purpose                                                                                   |
| ------------------------------------ | ----------------------------------------------------------------------------------------- |
| [Architecture](docs/ARCHITECTURE.md) | System design, orchestration, layers, configuration, audit, and pipeline responsibilities |
| [Data Model](docs/DATA_MODEL.md)     | Gold dimensions, facts, grain, keys, joins, unknown rows, and BI relationships            |
| [Setup](docs/SETUP.md)               | Local setup, environment variables, Airflow, Databricks job IDs, and execution flow       |
| [Testing](docs/TESTING.md)           | Testing scope, smoke tests, and current test limitations                                  |

---

## How to run locally

Create a local environment file:

```bash
cp .env.example .env
```

Update `.env` with local runtime values and Databricks job IDs.

Start the local Airflow environment:

```bash
docker compose up --build
```

Open Airflow:

```text
http://localhost:8080
```

Recommended first-run order:

```text
1. Bootstrap
2. Smoke test
3. Bronze
4. Silver
5. Gold
```

For full setup instructions, see:

[Setup](docs/SETUP.md)

---

## Current project scope

This is a portfolio-grade project, not a full enterprise deployment package.

It demonstrates realistic design and implementation patterns, but some production concerns are intentionally limited or documented as future improvements.

Current limitations:

```text
full production deployment is not included
CI/CD should be expanded
integration tests should be expanded
infrastructure provisioning is not fully automated
monitoring and alerting could be extended
data volumes are portfolio-scale
```

---

## What this project demonstrates

This project is intended to demonstrate the following Data Engineering skills:

```text
lakehouse architecture
Databricks and Spark development
Delta Lake table design
Airflow orchestration
API ingestion patterns
file ingestion patterns
data quality handling
quarantine design
SCD2 implementation
incremental processing
audit logging
dimensional modeling
Gold fact/dimension design
configuration management
secret-management awareness
testing and smoke validation
```

---

## Target outcome

The target outcome is a working, understandable, and interview-defensible lakehouse project that shows how raw billing and ERP-style data can be transformed into a clean BI-ready analytical model.

The project is intentionally focused: it does not attempt to replicate every Stripe object or build a complete enterprise platform.

Instead, it focuses on the core patterns a Data Engineer should be able to explain, implement, and defend.

`````