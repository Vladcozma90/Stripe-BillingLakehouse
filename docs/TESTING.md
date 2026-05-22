# Testing

This document describes the current testing setup for `Stripe-BillingLakehouse`.

The project currently has two validation layers:

1. Unit tests for Python services, configuration loading, and job imports.
2. Smoke tests for the Airflow-to-Databricks orchestration path.

The tests are not meant to prove that the full production pipeline is complete. They are meant to catch basic runtime, packaging, configuration, and orchestration issues before running the real pipeline.

---

## Unit tests

Unit tests are stored in:

```text
tests/unit/
````

Current files:

```text
tests/unit/test_dq.py
tests/unit/test_envs.py
tests/unit/test_job_imports.py
```

### `test_dq.py`

This file tests the data quality helper functions in:

```text
src/services/dq.py
```

It checks that:

```text
invalid records fail the expected DQ rules
valid records pass the configured DQ rules
invalid dates and timestamps are handled as DQ failures
generated DQ failure messages include useful rule information
```

One issue found during testing was that malformed date and timestamp values caused Spark `CAST_INVALID_INPUT` errors. The DQ logic was updated to use `try_cast`, so invalid date and timestamp values are now counted as failed DQ checks instead of crashing the Spark job.

### `test_envs.py`

This file checks that the project configuration can be loaded correctly.

It validates the expected structure of the configuration file, including:

```text
paths
schemas
dataset-level settings
runtime configuration values
```

### `test_job_imports.py`

This file imports job entrypoint modules without executing them.

The purpose is to catch broken imports early. This is useful because Airflow and Databricks execute the code in a different runtime than the local IDE.

During testing, this caught imports like:

```python
from services...
```

These were changed to package-safe imports:

```python
from src.services...
```

---

## Running unit tests

The unit tests are run inside the Airflow scheduler container:

```powershell
docker compose exec airflow-scheduler uv run pytest tests/unit
```

Short output:

```powershell
docker compose exec airflow-scheduler uv run pytest tests/unit -q
```

Latest result:

```text
...........                                                              [100%]
11 passed in 10.47s
```

The saved result is available at:

```text
docs/screenshots/unit_tests_result.txt
```

The tests are run inside Docker because this is closer to the local runtime used by Airflow. It also verifies that Java, PySpark, project imports, and Airflow-related dependencies are available inside the container.

---

## Smoke tests

Smoke tests are used to check that the orchestration path works.

The current smoke test validates this flow:

```text
Airflow DAG
    -> DatabricksRunNowOperator
    -> Databricks Job
    -> Databricks task execution
    -> Delta table output
```

The Airflow connection used for Databricks is:

```text
databricks_default
```

The Databricks smoke-test job ID is passed through:

```text
DATABRICKS_SMOKE_TEST_JOB_ID
```

The smoke test confirms that:

```text
Airflow can connect to Databricks
Airflow can submit a Databricks job run
Airflow can monitor the Databricks run until completion
Databricks can execute the project code
the Databricks runtime can write test Delta tables
```

Smoke tests validate execution and connectivity. They do not validate the full business logic of the Bronze, Silver, and Gold pipelines.

---

## Evidence

Test evidence is stored under:

```text
docs/screenshots/
```

Current or recommended evidence files:

```text
unit_tests_result.txt
airflow_smoke_dag_success.png
airflow_databricks_task_log.png
databricks_smoke_job_success.png
databricks_smoke_tables.png
```

Before committing screenshots, sensitive values should be hidden where needed.

Examples of sensitive values:

```text
tokens
user emails
workspace IDs
workspace URLs
storage account names
secret names
```

---

## Current status

Current validation status:

```text
Unit tests: passed
Airflow smoke DAG: passed
Databricks smoke job: passed
```

Latest unit test result:

```text
11 passed in 10.47s
```

---

## Current limitations

The current tests do not yet validate the complete production pipeline.

They do not fully cover:

```text
real ADLS-backed ingestion
real Stripe API extraction
complete Bronze/Silver/Gold transformations
full business reconciliation
retry and recovery behavior
production-scale data volumes
end-to-end data validation across all layers
```

These checks should be added during the integration testing stage once the real Databricks jobs are fully wired and validated.

---

## Next testing steps

The next step is to validate the real Databricks jobs individually:

```text
billinglakehouse_bootstrap
billinglakehouse_bronze
billinglakehouse_silver
billinglakehouse_gold
```

Recommended validation order:

```text
1. Run bootstrap and confirm schemas/tables exist.
2. Run Bronze and confirm raw Delta tables are populated.
3. Run Silver and confirm DQ, quarantine, current, and conform outputs.
4. Run Gold and confirm dimensions and facts are populated.
5. Trigger the full pipeline from Airflow.
6. Compare row counts and key business metrics across layers.
```

After the individual jobs are validated, they can be orchestrated from Airflow as a full pipeline.

```