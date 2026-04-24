from __future__ import annotations

import uuid
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    current_timestamp,
    current_date,
)

from services.envs import EnvConfig
from services.audit import (
    insert_run_log_start,
    update_run_log_no_new_data,
    update_run_log_success,
    update_run_log_failure,
)
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "silver_conform_table": f"{env.catalog}.{env.project}_silver.s_conform_stripe_subscriptions",
        "gold_table": f"{env.catalog}.{env.project}_gold.g_dim_subscription",
        "gold_path": f"{env.curated_base_path}/{env.project}/stripe_subscriptions/g_dim_subscription",
    }


def _get_required_columns() -> list[str]:
    return [
        "stripe_subscriptions_sk",
        "subscription_id",
        "stripe_customer_id",
        "subscription_status",
        "collection_method",
        "currency",
        "latest_invoice_id",
        "account_id",
        "created_ts",
        "start_date_ts",
        "billing_cycle_anchor_ts",
        "cancel_at_ts",
        "canceled_at_ts",
        "ended_at_ts",
        "trial_start_ts",
        "trial_end_ts",
        "cancel_at_period_end",
        "livemode",
        "api_extracted_ts",
        "silver_effective_start_ts",
        "silver_effective_end_ts",
        "is_current",
        "etl_run_id",
    ]


def _build_gold_dim_subscription(silver_conform_df: DataFrame) -> DataFrame:
    return (
        silver_conform_df
        .withColumn("subscription_id", trim(col("subscription_id")).cast("string"))
        .withColumn("stripe_customer_id", trim(col("stripe_customer_id")).cast("string"))
        .withColumn("subscription_status", lower(trim(col("subscription_status"))).cast("string"))
        .withColumn("collection_method", lower(trim(col("collection_method"))).cast("string"))
        .withColumn("currency", lower(trim(col("currency"))).cast("string"))
        .withColumn("latest_invoice_id", trim(col("latest_invoice_id")).cast("string"))
        .withColumn("account_id", trim(col("account_id")).cast("string"))
        .withColumn("created_ts", col("created_ts").cast("timestamp"))
        .withColumn("start_date_ts", col("start_date_ts").cast("timestamp"))
        .withColumn("billing_cycle_anchor_ts", col("billing_cycle_anchor_ts").cast("timestamp"))
        .withColumn("cancel_at_ts", col("cancel_at_ts").cast("timestamp"))
        .withColumn("canceled_at_ts", col("canceled_at_ts").cast("timestamp"))
        .withColumn("ended_at_ts", col("ended_at_ts").cast("timestamp"))
        .withColumn("trial_start_ts", col("trial_start_ts").cast("timestamp"))
        .withColumn("trial_end_ts", col("trial_end_ts").cast("timestamp"))
        .withColumn("cancel_at_period_end", col("cancel_at_period_end").cast("boolean"))
        .withColumn("livemode", col("livemode").cast("boolean"))
        .withColumn("api_extracted_ts", col("api_extracted_ts").cast("timestamp"))
        .withColumn("gold_loaded_ts", current_timestamp())
        .withColumn("gold_loaded_date", current_date())
        .withColumn("etl_run_id", col("etl_run_id").cast("string"))
        .select(
            "stripe_subscriptions_sk",
            "subscription_id",
            "stripe_customer_id",
            "subscription_status",
            "collection_method",
            "currency",
            "latest_invoice_id",
            "account_id",
            "created_ts",
            "start_date_ts",
            "billing_cycle_anchor_ts",
            "cancel_at_ts",
            "canceled_at_ts",
            "ended_at_ts",
            "trial_start_ts",
            "trial_end_ts",
            "cancel_at_period_end",
            "livemode",
            "api_extracted_ts",
            "silver_effective_start_ts",
            "silver_effective_end_ts",
            "is_current",
            "gold_loaded_ts",
            "gold_loaded_date",
            "etl_run_id",
        )
    )


def run_gold_dim_subscription(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "gold_dim_subscription"
    dataset = "dim_subscription"
    run_id = uuid.uuid4().hex

    cfg = _build_config(env)

    rows_in = 0
    rows_out = 0

    insert_run_log_start(
        spark=spark,
        run_logs_table=cfg["run_logs_table"],
        pipeline_name=pipeline_name,
        dataset=dataset,
        target_table=cfg["gold_table"],
        run_id=run_id,
    )

    try:
        logger.info("Gold dim_subscription start | run_id=%s", run_id)

        silver_conform_df = spark.table(cfg["silver_conform_table"])

        required_columns = _get_required_columns()
        missing_columns = [c for c in required_columns if c not in silver_conform_df.columns]
        if missing_columns:
            raise ValueError(f"Silver conform missing required cols: {missing_columns}")

        if silver_conform_df.isEmpty():
            update_run_log_no_new_data(
                spark=spark,
                run_logs_table=cfg["run_logs_table"],
                pipeline_name=pipeline_name,
                dataset=dataset,
                run_id=run_id,
                last_watermark_ts=None,
            )
            logger.info("No data in silver conform. Exiting.")
            return

        gold_df = _build_gold_dim_subscription(
            silver_conform_df=silver_conform_df,
        )

        rows_in = silver_conform_df.count()
        rows_out = gold_df.count()

        gold_dt = DeltaTable.forName(spark, cfg["gold_table"])

        (
            gold_dt.alias("t")
            .merge(
                gold_df.alias("s"),
                "t.stripe_subscriptions_sk = s.stripe_subscriptions_sk",
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
            "Gold dim_subscription SUCCESS | rows_in=%d | rows_out=%d",
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
        logger.exception("Gold dim_subscription FAILED")
        raise