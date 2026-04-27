from __future__ import annotations

import uuid
import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    current_timestamp,
    current_date,
    lit,
    coalesce,
)

from services.envs import EnvConfig
from services.audit import (
    insert_run_log_start,
    update_run_log_no_new_data,
    update_run_log_success,
    update_run_log_failure,
)

logger = logging.getLogger(__name__)

def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",

        "silver_current_table": f"{env.catalog}.{env.project}_silver.s_current_stripe_subscription_items",

        "gold_dim_subscription_table": f"{env.catalog}.{env.project}_gold.g_dim_subscription",
        "gold_dim_customer_table": f"{env.catalog}.{env.project}_gold.g_dim_customer",

        "gold_fact_table": f"{env.catalog}.{env.project}_gold.g_fact_subscription_item",
        "gold_fact_path": f"{env.gold_base_path}/{env.catalog}/{env.project}/stripe_subscription_items/g_fact_subscription_item",
    }


def _get_required_columns() -> list[str]:
    return [
        "subscription_item_id",
        "subscription_id",
        "stripe_customer_id",
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
        "_file_name",
        "_source",
        "_landing_format",
        "etl_run_id",
    ]


def _build_stage_gold_fact_subscription_items(
        silver_df: DataFrame,
        dim_subscriptions_df: DataFrame,
        dim_customers_df: DataFrame,
) -> DataFrame:
    stage_df = (
        silver_df
        .withColumn("subscription_item_id", trim(col("subscription_item_id")).cast("string"))
        .withColumn("subscription_id", trim(col("subscription_id")).cast("string"))
        .withColumn("stripe_customer_id", trim(col("stripe_customer_id")).cast("string"))
        .withColumn("price_id", trim(col("price_id")).cast("string"))
        .withColumn("product_id", trim(col("product_id")).cast("string"))
        .withColumn("item_currency", lower(trim(col("item_currency"))).cast("string"))
        .withColumn("billing_interval", lower(trim(col("billing_interval"))).cast("string"))
        .withColumn("price_type", lower(trim(col("price_type"))).cast("string"))
        .withColumn("usage_type", lower(trim(col("usage_type"))).cast("string"))
        .withColumn("quantity", col("quantity").cast("bigint"))
        .withColumn("unit_amount", col("unit_amount").cast("bigint"))
        .withColumn("item_created_ts", col("item_created_ts").cast("timestamp"))
        .withColumn("item_current_period_start_ts", col("item_current_period_start_ts").cast("timestamp"))
        .withColumn("item_current_period_end_ts", col("item_current_period_end_ts").cast("timestamp"))
        .withColumn("api_extracted_ts", col("api_extracted_ts").cast("timestamp"))
    )

    subscription_current_df = (
        dim_subscriptions_df
        .filter(col("is_current") == True)
        .select(
            col("stripe_subscriptions_sk"),
            col("subscription_id").alias("dim_subscription_id"),
        )
    )

    customer_current_df = (
        dim_customers_df
        .filter(col("is_current") == True)
        .select(
            col("stripe_customers_sk"),
            col("customer_id").alias("dim_customer_id")
        )
    )

    joined_df = (
        stage_df.alias("f")
        .join(
            subscription_current_df.alias("s"),
            col("f.subscription_id") == col("s.dim_subscription_id"),
            "left"
        )
        .join(
            customer_current_df.alias("c"),
            col("f.stripe_customer_id") == col("c.dim_customer_id"),
            "left"
        )
    )

    return (
        joined_df
        .withColumn("stripe_subscriptions_sk", coalesce(col("s.stripe_subscriptions_sk"), lit(-1)))
        .withColumn("stripe_customers_sk", coalesce(col("c.stripe_customers_sk"), lit(-1)))
        .withColumn("gold_loaded_ts", current_timestamp())
        .withColumn("gold_loaded_date", current_date())
        .select(
            col("f.subscription_item_id").alias("subscription_item_id"),
            col("stripe_subscriptions_sk"),
            col("stripe_customers_sk"),
            col("f.subscription_id").alias("subscription_id"),
            col("f.stripe_customer_id").alias("stripe_customer_id"),
            col("f.price_id").alias("price_id"),
            col("f.product_id").alias("product_id"),
            col("f.item_currency").alias("item_currency"),
            col("f.billing_interval").alias("billing_interval"),
            col("f.price_type").alias("price_type"),
            col("f.usage_type").alias("usage_type"),
            col("f.quantity").alias("quantity"),
            col("f.unit_amount").alias("unit_amount"),
            col("f.item_created_ts").alias("item_created_ts"),
            col("f.item_current_period_start_ts").alias("item_current_period_start_ts"),
            col("f.item_current_period_end_ts").alias("item_current_period_end_ts"),
            col("f.api_extracted_ts").alias("api_extracted_ts"),
            col("f._file_name").alias("_file_name"),
            col("f._source").alias("_source"),
            col("f._landing_format").alias("_landing_format"),
            col("f.etl_run_id").alias("etl_run_id"),
            col("gold_loaded_ts"),
            col("gold_loaded_date"),
        )
    )


def run_gold_fact_subscription_item(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "gold_fact_subscription_item"
    dataset = "fact_subscription_item"
    run_id = uuid.uuid4().hex

    cfg = _build_config(env)

    rows_in = 0
    rows_out = 0

    insert_run_log_start(
        spark=spark,
        run_logs_table=cfg["run_logs_table"],
        pipeline_name=pipeline_name,
        dataset=dataset,
        target_table=cfg["gold_fact_table"],
        run_id=run_id,
    )

    try:
        logger.info("Gold fact_subscription_item start | run_id=%s", run_id)

        silver_df = spark.table(cfg["silver_current_table"])
        dim_subscription_df = spark.table(cfg["gold_dim_subscription_table"])
        dim_customer_df = spark.table(cfg["gold_dim_customer_table"])

        required_columns = _get_required_columns()
        missing_columns = [c for c in required_columns if c not in silver_df.columns]
        if missing_columns:
            raise ValueError(f"Silver current missing required cols: {missing_columns}")

        if silver_df.isEmpty():
            update_run_log_no_new_data(
                spark=spark,
                run_logs_table=cfg["run_logs_table"],
                pipeline_name=pipeline_name,
                dataset=dataset,
                run_id=run_id,
                last_watermark_ts=None,
            )
            logger.info("No data in silver current. Exiting.")
            return

        gold_df = _build_stage_gold_fact_subscription_items(
            silver_df=silver_df,
            dim_subscription_df=dim_subscription_df,
            dim_customer_df=dim_customer_df,
        )

        rows_in = silver_df.count()
        rows_out = gold_df.count()

        gold_dt = DeltaTable.forName(spark, cfg["gold_fact_table"])

        (
            gold_dt.alias("t")
            .merge(
                gold_df.alias("s"),
                "t.subscription_item_id = s.subscription_item_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        update_run_log_success(
            spark=spark,
            run_logs_table=cfg["run_logs_table"],
            pipeline_name=pipeline_name,
            dataset=dataset,
            run_id=run_id,
            dq_result=None,
            rows_in=rows_in,
            rows_out=rows_out,
            rows_quarantined=0,
            last_watermark_ts=None,
        )

        logger.info(
            "Gold fact_subscription_item SUCCESS | rows_in=%d | rows_out=%d",
            rows_in,
            rows_out,
        )

    except Exception as e:
        update_run_log_failure(
            spark=spark,
            run_logs_table=cfg["run_logs_table"],
            pipeline_name=pipeline_name,
            dataset=dataset,
            run_id=run_id,
            error_msg=str(e),
            rows_in=rows_in,
            rows_out=rows_out,
            rows_quarantined=0,
            dq_result="ERROR",
            last_watermark_ts=None,
        )
        logger.exception("Gold fact_subscription_item FAILED")
        raise