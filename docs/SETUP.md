# Setup

## Overview

This document explains how to configure and run the `Stripe-BillingLakehouse` project in a local development environment and how the project is expected to connect to Databricks.

The project is designed as a portfolio-grade data engineering pipeline using:

- Apache Airflow for orchestration
- Databricks for Spark / Delta Lake processing
- YAML files for non-secret configuration
- Environment variables for runtime settings
- Azure Key Vault pattern for secrets

This setup is intended for development and demonstration. It is not a full production deployment guide.

---

## Prerequisites

Before running the project, install or configure:

| Requirement | Purpose |
|---|---|
| Git | Clone the repository |
| Docker Desktop | Run local Airflow services |
| Python 3.12+ | Local development and testing |
| Databricks workspace | Run lakehouse jobs |
| Databricks Airflow connection | Allow Airflow to trigger Databricks jobs |
| Azure storage / ADLS | Store landing, raw, curated, and ops data |
| Azure Key Vault | Store secrets such as API tokens |

---

## Repository setup

Clone the repository:

```bash
git clone https://github.com/Vladcozma90/Stripe-BillingLakehouse.git
cd Stripe-BillingLakehouse
````

The expected project structure is:

```text
Stripe-BillingLakehouse/
|
├── bootstrap/
├── configs/
├── dags/
├── docs/
├── jobs/
├── src/
├── tests/
├── Dockerfile
├── docker-compose.yaml
├── pyproject.toml
└── README.md
```

---

## Environment variables

The project uses a `.env` file for local development.

Create it from the template:

```bash
cp .env.example .env
```

Then edit `.env` and replace placeholder values.

Expected variables:

```env
AIRFLOW_UID=50000
ENV=dev
LOG_LEVEL=INFO
APP_CONFIG_DIR=/opt/airflow/app/configs

DATABRICKS_CONN_ID=databricks_default

DATABRICKS_BRONZE_JOB_ID=replace_with_bronze_job_id
DATABRICKS_SILVER_JOB_ID=replace_with_silver_job_id
DATABRICKS_GOLD_JOB_ID=replace_with_gold_job_id
DATABRICKS_BOOTSTRAP_JOB_ID=replace_with_bootstrap_job_id
DATABRICKS_SMOKE_TEST_JOB_ID=replace_with_smoke_test_job_id
```

The `.env` file should not be committed to GitHub.

Only `.env.example` should be committed.

---

## Configuration files

Environment-specific project configuration is stored under:

```text
configs/
```

Example:

```text
configs/dev.yaml
```

The YAML configuration should contain non-secret project settings such as:

```text
catalog
schemas
storage paths
dataset names
source configuration
data quality rules
```

Environment variables should not duplicate YAML configuration unless the value is truly runtime-specific.

Recommended separation:

```text
.env file       -> runtime variables and local execution settings
YAML config     -> project configuration
Key Vault       -> secrets
```

---

## Secrets

Secrets should not be stored in:

```text
.env
configs/*.yaml
source code
documentation
```

Secrets should be stored in Azure Key Vault or the equivalent secret manager used by the deployment environment.

Examples of secrets:

```text
Stripe API token
Databricks token
Azure client secret
storage credentials
```

For local development, use safe test credentials only.

---

## Local Airflow setup

The project includes a Docker-based Airflow setup.

Build and start the local services:

```bash
docker compose up --build
```

Airflow should become available at:

```text
http://localhost:8080
```

Default local Airflow credentials depend on the Docker/Airflow initialization configuration. For local development, the typical default is:

```text
username: airflow
password: airflow
```

Stop the environment:

```bash
docker compose down
```

Stop and remove local volumes:

```bash
docker compose down -v
```

Use volume removal carefully because it deletes local Airflow metadata.

---

## Airflow connection to Databricks

Airflow must have a Databricks connection configured.

The connection ID should match:

```env
DATABRICKS_CONN_ID=databricks_default
```

Configure this connection in Airflow UI or through environment/secret-backed deployment configuration.

The connection should point to the Databricks workspace that contains the jobs used by the project.

---

## Databricks jobs

The project expects separate Databricks jobs for the main pipeline stages:

```text
bootstrap
bronze
silver
gold
smoke_test
```

The corresponding job IDs are passed through environment variables:

```env
DATABRICKS_BOOTSTRAP_JOB_ID=
DATABRICKS_BRONZE_JOB_ID=
DATABRICKS_SILVER_JOB_ID=
DATABRICKS_GOLD_JOB_ID=
DATABRICKS_SMOKE_TEST_JOB_ID=
```

These values are environment-specific and should not be hard-coded in the source code.

---

## Recommended execution order

For a fresh environment, run the jobs in this order:

```text
1. Bootstrap
2. Smoke test
3. Bronze
4. Silver
5. Gold
```

### 1. Bootstrap

Bootstrap prepares required schemas and tables.

Expected schemas:

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

Bootstrap should be safe to rerun and should not destroy existing data unless explicitly designed to do so.

### 2. Smoke test

The smoke test validates basic Databricks execution and Delta read/write behavior.

Smoke test objects are test artifacts and are not part of the core medallion architecture.

### 3. Bronze

Bronze loads source data into raw Delta tables.

### 4. Silver

Silver validates, cleans, deduplicates, quarantines, and conforms Bronze data.

### 5. Gold

Gold builds BI-ready dimensions and facts.

---

## Running the Airflow DAG

After the Docker environment is running:

1. Open Airflow at `http://localhost:8080`
2. Confirm the Databricks connection exists
3. Confirm the required job IDs are available in `.env`
4. Enable the DAG
5. Trigger the DAG manually for the first test run

For the first execution, prefer running the smoke test first before triggering the full pipeline.

---

## Local testing

Run tests from the repository root:

```bash
pytest
```

If using `uv`:

```bash
uv run pytest
```

The tests are intended to validate selected logic such as:

```text
configuration loading
import safety
data quality logic
smoke-test behavior
```

They do not replace a full integration test against a real Databricks workspace and ADLS environment.

---

## Development workflow

Recommended local development flow:

```text
1. Make code changes locally.
2. Run unit tests.
3. Build the Docker image.
4. Start local Airflow.
5. Validate DAG import.
6. Trigger smoke test.
7. Trigger the relevant Databricks job.
8. Review run logs and target tables.
```

---

## Git hygiene

The following files should be committed:

```text
.env.example
configs/dev.yaml or configs/dev.example.yaml
source code
tests
documentation
```

The following files should not be committed:

```text
.env
real secrets
local logs
temporary files
personal Databricks tokens
real production credentials
```

Recommended `.gitignore` behavior:

```gitignore
.env
.env.*
!.env.example
logs/
__pycache__/
.pytest_cache/
```

---

## Production notes

In production, `.env` files are normally not used.

Runtime variables should be injected by the platform, for example:

```text
Kubernetes Secrets / ConfigMaps
Azure Container Apps environment variables
Jenkins credentials
GitHub Actions secrets
Airflow connections
Databricks job parameters
Terraform-managed secret references
```

Production secrets should be stored in a secret manager, not in Git.

The production deployment should also include:

```text
CI/CD
secret management
infrastructure provisioning
monitoring and alerting
access control
environment separation
```

Those topics are outside the scope of this portfolio setup guide.

---

## Troubleshooting

### Airflow starts but DAG cannot trigger Databricks

Check:

```text
DATABRICKS_CONN_ID
Databricks connection configuration
Databricks job IDs
network access from Airflow to Databricks
```

### Databricks job ID is missing or invalid

Check the `.env` file:

```env
DATABRICKS_BRONZE_JOB_ID=
DATABRICKS_SILVER_JOB_ID=
DATABRICKS_GOLD_JOB_ID=
DATABRICKS_BOOTSTRAP_JOB_ID=
```

Do not use `0` as a real job ID.

### Config file cannot be found

Check:

```env
APP_CONFIG_DIR=/opt/airflow/app/configs
ENV=dev
```

The application should be able to resolve:

```text
/opt/airflow/app/configs/dev.yaml
```

inside the Airflow container.

### Secrets cannot be loaded

Check:

```text
Azure authentication
Key Vault URL
secret names
permissions
local credentials
managed identity configuration
```

### Tables are created in the wrong schema

Check that the code consistently uses configured schema names from YAML instead of hard-coded schema names.

Expected schemas:

```text
bronze
silver
gold
ops
```

---

## Current setup scope

This setup supports a realistic portfolio demonstration.

It is expected to show:

```text
Airflow orchestration
Databricks job triggering
configuration-driven execution
medallion architecture
audit logging
smoke testing
Gold dimensional outputs
```

It is not intended to be a complete enterprise deployment package.

```

This matches your repo’s actual direction: Docker-based Airflow, environment variables for Databricks job IDs, YAML config for project settings, and separate folders for `bootstrap`, `configs`, `dags`, `jobs`, `src`, and `tests`. The repo currently exposes that structure and uses Docker Compose with `.env`-driven variables for `ENV`, `LOG_LEVEL`, `APP_CONFIG_DIR`, `DATABRICKS_CONN_ID`, and the Databricks job IDs. 
```