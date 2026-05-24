from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_default")

API_EXTRACT_JOB_ID = int(os.environ["DATABRICKS_API_EXTRACT_JOB_ID"])
BRONZE_JOB_ID = int(os.environ["DATABRICKS_BRONZE_JOB_ID"])
SILVER_JOB_ID = int(os.environ["DATABRICKS_SILVER_JOB_ID"])
GOLD_JOB_ID = int(os.environ["DATABRICKS_GOLD_JOB_ID"])

@dag(
    dag_id="billinglakehouse_pipeline",
    description="Stripe Billing Lakehouse pipeline",
    start_date=datetime(2026, 6, 26),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["stripe", "lakehouse", "billing"]
)
def billinglakehouse_pipeline():

    trigger_API_extract = DatabricksRunNowOperator(
        task_id="trigger_databricks_api_extract",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=API_EXTRACT_JOB_ID,
    )

    trigger_bronze = DatabricksRunNowOperator(
        task_id="trigger_databricks_bronze",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BRONZE_JOB_ID,
    )

    trigger_silver = DatabricksRunNowOperator(
        task_id="trigger_databricks_silver",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=SILVER_JOB_ID,
    )

    trigger_gold = DatabricksRunNowOperator(
        task_id="trigger_databricks_gold",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=GOLD_JOB_ID,
    )

    trigger_API_extract >> trigger_bronze >> trigger_silver >> trigger_gold

billinglakehouse_pipeline()