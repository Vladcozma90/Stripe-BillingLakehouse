from __future__ import annotations
import os
from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_default")

BOOTSTRAP_SMOKE_TEST_TEST_JOB_ID = int(
    os.getenv("BOOTSTRAP_SMOKE_TEST_JOB_ID", "0")
)

E2E_SMOKE_TEST_JOB_ID = int(
    os.getenv("DATABRICKS_E2E_SMOKE_TEST_JOB_ID", "0")
)

@dag(
    dag_id="billinglakehouse_smoke_tests_dag",
    description="Manual smoke-test DAG for Databricks runtime and E2E lakehouse validation",
    start_date=datetime(2026, 6, 26),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["stripe", "lakehouse", "databricks", "smoke-test"],
)
def billinglakehouse_smoke_tests_dag():

    trigger_bootstrap_test = DatabricksRunNowOperator(
        task_id="trigger_bootstrap_smoke_test",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BOOTSTRAP_SMOKE_TEST_TEST_JOB_ID,
    )

    trigger_e2e_smoke_test = DatabricksRunNowOperator(
        task_id="trigger_e2e_smoke_test",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=E2E_SMOKE_TEST_JOB_ID,
    )

    trigger_bootstrap_test >> trigger_e2e_smoke_test

billinglakehouse_smoke_tests_dag()