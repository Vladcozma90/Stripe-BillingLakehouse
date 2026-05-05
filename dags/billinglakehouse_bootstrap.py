from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_default")
BOOTSTRAP_JOB_ID = os.getenv("DATABRICKS_BOOTSTRAP_JOB_ID", "0")

@dag(
    dag_id="billinglakehouse_bootstrap",
    description="Manual bootstrap job for schemas, tables and operational metadata",
    start_date=datetime(2026, 6, 26),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["stripe", "lakehouse", "bootstrap"],
)
def billinghouselake_bootstrap():

    DatabricksRunNowOperator(
        task_id="trigger_databricks_bootstrap",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BOOTSTRAP_JOB_ID,
    )
billinghouselake_bootstrap()