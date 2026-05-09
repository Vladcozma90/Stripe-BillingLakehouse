from __future__ import annotations

import argparse
import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    lower,
    round as spark_round,
    sum as spark_sum,
    trim,
    upper,
)


def _get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def run_e2e_smoke_test() -> None:
    args = _get_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s",
    )

    logger = logging.getLogger(__name__)

    spark = SparkSession.builder.getOrCreate()

    catalog = args.catalog
    schema = args.schema

    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc).isoformat()

    landing_table = f"{catalog}.{schema}.e2e_landing_source"
    bronze_table = f"{catalog}.{schema}.e2e_bronze_customers"
    silver_table = f"{catalog}.{schema}.e2e_silver_customers"
    gold_table = f"{catalog}.{schema}.e2e_gold_revenue_by_country"

    logger.info(
        "Starting E2E smoke test | catalog=%s | schema=%s | run_id=%s",
        catalog,
        schema,
        run_id,
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # -------------------------------------------------------------------------
    # 1. Landing-style source data
    # -------------------------------------------------------------------------
    landing_df = spark.createDataFrame(
        [
            ("cus_001 ", " CUSTOMER_1@EXAMPLE.COM ", " us ", 100.00, started_at),
            ("cus_002", "customer_2@example.com", " ro", 250.00, started_at),
            ("cus_003", "CUSTOMER_3@EXAMPLE.COM", "de ", 150.00, started_at),
            ("cus_004", "customer_4@example.com", "us", 50.00, started_at),
        ],
        [
            "customer_id",
            "email",
            "country_code",
            "invoice_amount_usd",
            "source_created_at",
        ],
    )

    landing_df.write.mode("overwrite").saveAsTable(landing_table)

    landing_count = spark.table(landing_table).count()

    if landing_count != 4:
        raise RuntimeError(
            f"E2E smoke test failed at landing step. Expected 4 rows, got {landing_count}."
        )

    logger.info("Landing step completed | table=%s | rows=%s", landing_table, landing_count)

    # -------------------------------------------------------------------------
    # 2. Bronze ingestion
    # -------------------------------------------------------------------------
    bronze_df = (
        spark.table(landing_table)
        .withColumn("_bronze_ingest_ts", current_timestamp())
        .withColumn("_source_system", lit("smoke_test"))
        .withColumn("_source_dataset", lit("stripe_customers"))
        .withColumn("_run_id", lit(run_id))
    )

    bronze_df.write.mode("overwrite").saveAsTable(bronze_table)

    bronze_count = spark.table(bronze_table).count()

    if bronze_count != 4:
        raise RuntimeError(
            f"E2E smoke test failed at bronze step. Expected 4 rows, got {bronze_count}."
        )

    logger.info("Bronze step completed | table=%s | rows=%s", bronze_table, bronze_count)

    # -------------------------------------------------------------------------
    # 3. Silver cleaning / standardization
    # -------------------------------------------------------------------------
    silver_df = (
        spark.table(bronze_table)
        .select(
            trim(col("customer_id")).alias("customer_id"),
            lower(trim(col("email"))).alias("email"),
            upper(trim(col("country_code"))).alias("country_code"),
            col("invoice_amount_usd").cast("double").alias("invoice_amount_usd"),
            col("source_created_at"),
            col("_bronze_ingest_ts"),
            col("_source_system"),
            col("_source_dataset"),
            col("_run_id"),
        )
        .withColumn("_silver_processed_ts", current_timestamp())
    )

    silver_df.write.mode("overwrite").saveAsTable(silver_table)

    result_silver_df = spark.table(silver_table)

    silver_count = result_silver_df.count()
    null_customer_count = result_silver_df.filter(col("customer_id").isNull()).count()
    invalid_country_count = result_silver_df.filter(
        ~col("country_code").isin("US", "RO", "DE")
    ).count()

    if silver_count != 4:
        raise RuntimeError(
            f"E2E smoke test failed at silver step. Expected 4 rows, got {silver_count}."
        )

    if null_customer_count > 0:
        raise RuntimeError(
            f"E2E smoke test failed at silver step. Null customer_id count={null_customer_count}."
        )

    if invalid_country_count > 0:
        raise RuntimeError(
            f"E2E smoke test failed at silver step. Invalid country count={invalid_country_count}."
        )

    logger.info("Silver step completed | table=%s | rows=%s", silver_table, silver_count)

    # -------------------------------------------------------------------------
    # 4. Gold aggregation
    # -------------------------------------------------------------------------
    gold_df = (
        result_silver_df
        .groupBy("country_code")
        .agg(
            spark_round(spark_sum("invoice_amount_usd"), 2).alias("total_revenue_usd")
        )
        .withColumn("_gold_processed_ts", current_timestamp())
    )

    gold_df.write.mode("overwrite").saveAsTable(gold_table)

    result_gold_df = spark.table(gold_table)

    gold_count = result_gold_df.count()

    expected_results = {
        row["country_code"]: float(row["total_revenue_usd"])
        for row in result_gold_df.select("country_code", "total_revenue_usd").collect()
    }

    expected = {
        "US": 150.00,
        "RO": 250.00,
        "DE": 150.00,
    }

    if gold_count != 3:
        raise RuntimeError(
            f"E2E smoke test failed at gold step. Expected 3 country rows, got {gold_count}."
        )

    if expected_results != expected:
        raise RuntimeError(
            f"E2E smoke test failed at gold step. Expected {expected}, got {expected_results}."
        )

    logger.info(
        "Gold step completed | table=%s | rows=%s | results=%s",
        gold_table,
        gold_count,
        expected_results,
    )

    logger.info(
        "E2E smoke test finished successfully | run_id=%s | landing=%s | bronze=%s | silver=%s | gold=%s",
        run_id,
        landing_table,
        bronze_table,
        silver_table,
        gold_table,
    )


if __name__ == "__main__":
    run_e2e_smoke_test()