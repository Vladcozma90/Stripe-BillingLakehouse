from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/app")
ENV = os.getenv("ENV", "dev")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_default")

BRONZE_JOB_ID = int(os.getenv("DATABRICKS_BRONZE_JOB_ID", "0"))
SILVER_JOB_ID = int(os.getenv("DATABRICKS_SILVER_JOB_ID", "0"))
GOLD_JOB_ID = int(os.getenv("DATABRICKS_GOLD_JOB_ID", "0"))

DEFAULT_ENV = {
    "PYTHONPATH": PROJECT_ROOT,
    "ENV": ENV,
    "LOG_LEVEL": LOG_LEVEL,
}

STRIPE_REST_DATASETS = [
    "stripe_customers",
    "stripe_subscriptions",
    "stripe_subscription_items",
    "stripe_invoices",
]


@dag(
    dag_id="billinglakehouse_pipeline_dry_run",
    description="Stripe Billing Lakehouse pipeline",
    start_date=datetime(2026, 6, 26),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["stripe", "lakehouse", "billing"],
)
def billinglakehouse_pipeline():

    @task.bash(
        task_id="extract_rest_to_landing",
        env=DEFAULT_ENV,
        append_env=True,
    )
    def extract_rest_to_landing(dataset: str) -> str:
        if DRY_RUN:
            return f"echo 'DRY RUN: extract {dataset} to landing'"

        return (
            f"cd {PROJECT_ROOT} && "
            "python -m jobs.bronze.extract_rest_to_landing "
            f"--dataset {dataset}"
        )

    @task.bash(task_id="dry_run_trigger_bronze")
    def dry_run_trigger_bronze() -> str:
        return "echo 'DRY RUN: trigger Databricks bronze job'"

    @task.bash(task_id="dry_run_trigger_silver")
    def dry_run_trigger_silver() -> str:
        return "echo 'DRY RUN: trigger Databricks silver job'"

    @task.bash(task_id="dry_run_trigger_gold")
    def dry_run_trigger_gold() -> str:
        return "echo 'DRY RUN: trigger Databricks gold job'"

    rest_extract_tasks = extract_rest_to_landing.expand(
        dataset=STRIPE_REST_DATASETS
    )

    if DRY_RUN:
        trigger_bronze = dry_run_trigger_bronze()
        trigger_silver = dry_run_trigger_silver()
        trigger_gold = dry_run_trigger_gold()
    else:
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

    rest_extract_tasks >> trigger_bronze >> trigger_silver >> trigger_gold


billinglakehouse_pipeline()