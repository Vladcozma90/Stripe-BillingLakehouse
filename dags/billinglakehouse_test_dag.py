from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_default")
DATABRICKS_SMOKE_TEST_JOB_ID = int(os.getenv("DATABRICKS_SMOKE_TEST_JOB_ID", "0"))

if DATABRICKS_SMOKE_TEST_JOB_ID == 0:
    raise ValueError("DATABRICKS_SMOKE_TEST_JOB_ID is not set")


@dag(
    dag_id="billinglakehouse_databricks_smoke_test",
    description="Manual smoke-test DAG for Databricks runtime validation",
    start_date=datetime(2025, 6, 26),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["stripe", "lakehouse", "databricks", "smoke-test"],
)
def billinglakehouse_databricks_smoke_test():

    DatabricksRunNowOperator(
        task_id="trigger_databricks_smoke_test",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_SMOKE_TEST_JOB_ID,
    )


billinglakehouse_databricks_smoke_test()