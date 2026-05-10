from __future__ import annotations

import argparse
import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def _get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def run_bootstrap_test() -> None:
    args = _get_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    )

    logger = logging.getLogger(__name__)

    spark = SparkSession.builder.getOrCreate()

    catalog = args.catalog
    schema = args.schema

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
    ).withColumn("created_at", current_timestamp())

    df.write.mode("append").saveAsTable(
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