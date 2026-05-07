from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
)

logger = logging.getLogger(__name__)


def run_bootstrap_test() -> None:
    """
    Lightweight Databricks bootstrap test.

    This does not require ADLS, Key Vault, Stripe, or external locations.
    It validates that Databricks Jobs can run this repo and create
    managed Unity Catalog objects.
    """

    spark = SparkSession.builder.getOrCreate()

    catalog = os.getenv("DATABRICKS_TEST_CATALOG", "workspace")
    schema = os.getenv("DATABRICKS_TEST_SCHEMA", "billinglakehouse_test")

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    logger.info(
        "Starting bootstrap test | catalog=%s | schema=%s | run_id=%s",
        catalog,
        schema,
        run_id,
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bootstrap_smoke_test (
            run_id STRING,
            started_at STRING,
            test_name STRING,
            status STRING,
            created_at TIMESTAMP
        )
        USING DELTA
    """)

    df = spark.createDataFrame(
        [(run_id, started_at, "bootstrap_test", "SUCCESS")],
        ["run_id", "started_at", "test_name", "status"],
    ).withColumn(
        "created_at", current_timestamp()
    ).withColumn(
        "source", lit("databricks_job_test")
    )

    df.select(
        "run_id",
        "started_at",
        "test_name",
        "status",
        "created_at",
    ).write.mode("append").saveAsTable(
        f"{catalog}.{schema}.bootstrap_smoke_test"
    )

    row_count = spark.table(f"{catalog}.{schema}.bootstrap_smoke_test").count()

    logger.info(
        "Bootstrap test finished | table=%s.%s.bootstrap_smoke_test | row_count=%s",
        catalog,
        schema,
        row_count,
    )


if __name__ == "__main__":
    run_bootstrap_test()