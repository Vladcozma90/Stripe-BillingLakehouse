from __future__ import annotations
import uuid
import logging
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    lower,
    to_timestamp,
    from_unixtime,
    current_timestamp,
    current_date,
    sha2,
    concat_ws,
    coalesce
)

from services.envs import EnvConfig






logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table": f"{env.catalog}.{env.project}_ops.pipeline_state",
        "current_table": f"{env.catalog}.{env.project}_silver.s_current_stripe_subscription_items",
        "dq_table": f"{env.catalog}.{env.project}_silver.s_dq_stripe_subscription_items",
        "quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_stripe_subscription_items",
        "conform_table": f"{env.catalog}.{env.project}_silver.s_conform_stripe_subscription_items",

        # source is still subscriptions bronze, because items are nested inside it
        "bronze_path": f"{env.raw_base_path}/{env.project}/b_stripe_subscriptions",
        "current_path": f"{env.curated_base_path}/{env.project}/stripe_subscription_items/s_current_stripe_subscription_items",
        "dq_path": f"{env.curated_base_path}/{env.project}/stripe_subscription_items/s_dq_stripe_subscription_items",
        "quarantine_path": f"{env.curated_base_path}/{env.project}/stripe_subscription_items/s_quarantine_stripe_subscription_items",
        "conform_path": f"{env.curated_base_path}/{env.project}/stripe_subscription_items/s_conform_stripe_subscription_items",
    }


def _get_required_columns() -> list[str]:
    return [
        "_extracted_at",
        "date",
        "_ingest_date",
        "_file_name",
        "_source",
        "_landing_format",
    ]


def _build_stage_stripe_subscription_items(incr_df: DataFrame, run_id: str) -> DataFrame:
    df = (
        incr_df
        # identifiers
        .withColumn("subscription_item_id", col("data.id").cast("string"))
        .withColumn("subscription_id", col("data.subscription").cast("string"))

        # pricing
        .withColumn("price_id", col("data.price.id").cast("string"))
        .withColumn("product_id", col("data.price.product").cast("string"))
        .withColumn("item_currency", lower(trim(col("data.price.currency"))).cast("string"))
        .withColumn("unit_amount", col("data.price.unit_amount").cast("bigint"))
        .withColumn("billing_interval", lower(trim(col("data.price.recurring.interval"))).cast("string"))
        .withColumn("price_type", lower(trim(col("data.price.type"))).cast("string"))
        .withColumn("usage_type", lower(trim(col("data.price.recurring.usage_type"))).cast("string"))

        # quantity
        .withColumn("quantity", col("data.quantity").cast("bigint"))

        # timestamps
        .withColumn("item_created_ts", to_timestamp(from_unixtime(col("data.created"))))
        .withColumn("item_current_period_start_ts", to_timestamp(from_unixtime(col("data.current_period_start"))))
        .withColumn("item_current_period_end_ts", to_timestamp(from_unixtime(col("data.current_period_end"))))

        # ingestion metadata
        .withColumn("api_extracted_ts", to_timestamp(col("_extracted_at")))
        .withColumn("etl_run_id", lit(run_id))
        .withColumn("silver_processed_ts", current_timestamp())
        .withColumn("silver_processed_date", current_date())
    )

    return df.select(
        "subscription_item_id",
        "subscription_id",
        "price_id",
        "product_id",
        "item_currency",
        "billing_interval",
        "price_type",
        "usage_type",
        "quantity",
        "unit_amount",
        "item_created_ts",
        "item_current_period_start_ts",
        "item_current_period_end_ts",
        "api_extracted_ts",
        "_ingest_ts",
        "_ingest_date",
        "_file_name",
        "_source",
        "_landing_format",
        "etl_run_id",
        "silver_processed_ts",
        "silver_processed_date",
    )