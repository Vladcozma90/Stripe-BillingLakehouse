from __future__ import annotations

import uuid
import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    lower,
    upper,
    current_timestamp,
    current_date,
    sha2,
    concat_ws,
    coalesce,
)

from src.services.envs import EnvConfig
from src.services.audit import (
    insert_run_log_start,
    update_run_log_no_new_data,
    update_run_log_success,
    update_run_log_failure,
)


logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.schemas['ops']}.run_logs",
        "silver_conform_table": f"{env.catalog}.{env.schemas['silver']}.s_conform_stripe_subscriptions",
        "gold_table": f"{env.catalog}.{env.schemas['gold']}.g_dim_stripe_subscriptions",
    }


def _get_required_columns() -> list[str]:
    return [
        "subscription_id",
        "stripe_customer_id",
        "subscription_status",
        "collection_method",
        "currency",
        "latest_invoice_id",
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


def _validate_required_columns(
    df: DataFrame,
    required_columns: list[str],
    table_name: str,
) -> None:
    missing_columns = [c for c in required_columns if c not in df.columns]

    if missing_columns:
        raise ValueError(f"{table_name} missing required columns: {missing_columns}")


def _build_gold_dim_subscriptions(
    silver_conform_df: DataFrame,
    run_id: str,
) -> DataFrame:
    dim_df = (
        silver_conform_df
        .withColumn("subscription_business_key", trim(col("subscription_id")).cast("string"))
        .withColumn("subscription_id", trim(col("subscription_id")).cast("string"))
        .withColumn("stripe_customer_id", trim(col("stripe_customer_id")).cast("string"))
        .withColumn("subscription_status", lower(trim(col("subscription_status"))).cast("string"))
        .withColumn("collection_method", lower(trim(col("collection_method"))).cast("string"))
        .withColumn("currency", upper(trim(col("currency"))).cast("string"))
        .withColumn("latest_invoice_id", trim(col("latest_invoice_id")).cast("string"))
        .withColumn("created_ts", col("created_ts").cast("timestamp"))
        .withColumn("start_date_ts", col("start_date_ts").cast("timestamp"))
        .withColumn("billing_cycle_anchor_ts", col("billing_cycle_anchor_ts").cast("timestamp"))
        .withColumn("cancel_at_ts", col("cancel_at_ts").cast("timestamp"))
        .withColumn("canceled_at_ts", col("canceled_at_ts").cast("timestamp"))
        .withColumn("ended_at_ts", col("ended_at_ts").cast("timestamp"))
        .withColumn("trial_start_ts", col("trial_start_ts").cast("timestamp"))
        .withColumn("trial_end_ts", col("trial_end_ts").cast("timestamp"))
        .withColumn("cancel_at_period_end", col("cancel_at_period_end").cast("boolean"))
        .withColumn("is_livemode", col("livemode").cast("boolean"))
        .withColumn("api_extracted_ts", col("api_extracted_ts").cast("timestamp"))
        .withColumn("silver_effective_start_ts", col("silver_effective_start_ts").cast("timestamp"))
        .withColumn("silver_effective_end_ts", col("silver_effective_end_ts").cast("timestamp"))
        .withColumn("etl_run_id", lit(run_id).cast("string"))
        .withColumn("gold_processed_ts", current_timestamp())
        .withColumn("gold_processed_date", current_date())
        .filter(col("subscription_id") != lit("UNKNOWN"))
    )

    return (
        dim_df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("subscription_business_key").cast("string"), lit("")),
                    coalesce(col("subscription_id").cast("string"), lit("")),
                    coalesce(col("stripe_customer_id").cast("string"), lit("")),
                    coalesce(col("subscription_status").cast("string"), lit("")),
                    coalesce(col("collection_method").cast("string"), lit("")),
                    coalesce(col("currency").cast("string"), lit("")),
                    coalesce(col("latest_invoice_id").cast("string"), lit("")),
                    coalesce(col("created_ts").cast("string"), lit("")),
                    coalesce(col("start_date_ts").cast("string"), lit("")),
                    coalesce(col("billing_cycle_anchor_ts").cast("string"), lit("")),
                    coalesce(col("cancel_at_ts").cast("string"), lit("")),
                    coalesce(col("canceled_at_ts").cast("string"), lit("")),
                    coalesce(col("ended_at_ts").cast("string"), lit("")),
                    coalesce(col("trial_start_ts").cast("string"), lit("")),
                    coalesce(col("trial_end_ts").cast("string"), lit("")),
                    coalesce(col("cancel_at_period_end").cast("string"), lit("")),
                    coalesce(col("is_livemode").cast("string"), lit("")),
                    coalesce(col("api_extracted_ts").cast("string"), lit("")),
                    coalesce(col("silver_effective_start_ts").cast("string"), lit("")),
                    coalesce(col("silver_effective_end_ts").cast("string"), lit("")),
                ),
                256,
            )
        )
        .select(
            "subscription_business_key",
            "subscription_id",
            "stripe_customer_id",
            "subscription_status",
            "collection_method",
            "currency",
            "latest_invoice_id",
            "created_ts",
            "start_date_ts",
            "billing_cycle_anchor_ts",
            "cancel_at_ts",
            "canceled_at_ts",
            "ended_at_ts",
            "trial_start_ts",
            "trial_end_ts",
            "cancel_at_period_end",
            "is_livemode",
            "api_extracted_ts",
            "silver_effective_start_ts",
            "silver_effective_end_ts",
            "etl_run_id",
            "gold_processed_ts",
            "gold_processed_date",
            "record_hash",
        )
        .dropDuplicates(
            [
                "subscription_business_key",
                "silver_effective_start_ts",
            ]
        )
    )


def _merge_gold_dim_subscriptions(
    spark: SparkSession,
    target_table: str,
    source_df: DataFrame,
) -> None:
    target_dt = DeltaTable.forName(spark, target_table)

    merge_condition = " AND ".join(
        [
            "t.subscription_business_key <=> s.subscription_business_key",
            "t.silver_effective_start_ts <=> s.silver_effective_start_ts",
        ]
    )

    (
        target_dt.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition="NOT (t.record_hash <=> s.record_hash)",
            set={
                "subscription_id": "s.subscription_id",
                "stripe_customer_id": "s.stripe_customer_id",
                "subscription_status": "s.subscription_status",
                "collection_method": "s.collection_method",
                "currency": "s.currency",
                "latest_invoice_id": "s.latest_invoice_id",
                "created_ts": "s.created_ts",
                "start_date_ts": "s.start_date_ts",
                "billing_cycle_anchor_ts": "s.billing_cycle_anchor_ts",
                "cancel_at_ts": "s.cancel_at_ts",
                "canceled_at_ts": "s.canceled_at_ts",
                "ended_at_ts": "s.ended_at_ts",
                "trial_start_ts": "s.trial_start_ts",
                "trial_end_ts": "s.trial_end_ts",
                "cancel_at_period_end": "s.cancel_at_period_end",
                "is_livemode": "s.is_livemode",
                "api_extracted_ts": "s.api_extracted_ts",
                "silver_effective_end_ts": "s.silver_effective_end_ts",
                "etl_run_id": "s.etl_run_id",
                "gold_processed_ts": "s.gold_processed_ts",
                "gold_processed_date": "s.gold_processed_date",
                "record_hash": "s.record_hash",
            },
        )
        .whenNotMatchedInsert(
            values={
                "subscription_business_key": "s.subscription_business_key",
                "subscription_id": "s.subscription_id",
                "stripe_customer_id": "s.stripe_customer_id",
                "subscription_status": "s.subscription_status",
                "collection_method": "s.collection_method",
                "currency": "s.currency",
                "latest_invoice_id": "s.latest_invoice_id",
                "created_ts": "s.created_ts",
                "start_date_ts": "s.start_date_ts",
                "billing_cycle_anchor_ts": "s.billing_cycle_anchor_ts",
                "cancel_at_ts": "s.cancel_at_ts",
                "canceled_at_ts": "s.canceled_at_ts",
                "ended_at_ts": "s.ended_at_ts",
                "trial_start_ts": "s.trial_start_ts",
                "trial_end_ts": "s.trial_end_ts",
                "cancel_at_period_end": "s.cancel_at_period_end",
                "is_livemode": "s.is_livemode",
                "api_extracted_ts": "s.api_extracted_ts",
                "silver_effective_start_ts": "s.silver_effective_start_ts",
                "silver_effective_end_ts": "s.silver_effective_end_ts",
                "etl_run_id": "s.etl_run_id",
                "gold_processed_ts": "s.gold_processed_ts",
                "gold_processed_date": "s.gold_processed_date",
                "record_hash": "s.record_hash",
            },
        )
        .execute()
    )


def run_gold_dim_subscriptions(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "gold_dim_subscriptions"
    dataset = "dim_subscriptions"
    run_id = uuid.uuid4().hex

    cfg = _build_config(env)

    rows_in = 0
    rows_out = 0
    rows_quarantined = 0
    dq_result = "OK"

    gold_df = None

    insert_run_log_start(
        spark=spark,
        run_logs_table=cfg["run_logs_table"],
        pipeline_name=pipeline_name,
        dataset=dataset,
        target_table=cfg["gold_table"],
        run_id=run_id,
    )

    try:
        logger.info("Gold dim_subscriptions start | run_id=%s", run_id)

        silver_conform_df = spark.table(cfg["silver_conform_table"])

        _validate_required_columns(
            df=silver_conform_df,
            required_columns=_get_required_columns(),
            table_name=cfg["silver_conform_table"],
        )

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

        rows_in = silver_conform_df.filter(
            trim(col("subscription_id")) != lit("UNKNOWN")
        ).count()

        gold_df = _build_gold_dim_subscriptions(
            silver_conform_df=silver_conform_df,
            run_id=run_id,
        ).persist()

        rows_out = gold_df.count()

        if rows_out == 0:
            update_run_log_success(
                spark=spark,
                run_logs_table=cfg["run_logs_table"],
                pipeline_name=pipeline_name,
                dataset=dataset,
                run_id=run_id,
                dq_result=dq_result,
                rows_in=rows_in,
                rows_out=rows_out,
                rows_quarantined=rows_quarantined,
                last_watermark_ts=None,
            )

            logger.info("No subscription rows to merge | run_id=%s", run_id)
            return

        _merge_gold_dim_subscriptions(
            spark=spark,
            target_table=cfg["gold_table"],
            source_df=gold_df,
        )

        update_run_log_success(
            spark=spark,
            run_logs_table=cfg["run_logs_table"],
            pipeline_name=pipeline_name,
            dataset=dataset,
            run_id=run_id,
            dq_result=dq_result,
            rows_in=rows_in,
            rows_out=rows_out,
            rows_quarantined=rows_quarantined,
            last_watermark_ts=None,
        )

        logger.info(
            "Gold dim_subscriptions SUCCESS | rows_in=%d | rows_out=%d",
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
            rows_quarantined=rows_quarantined,
            dq_result=dq_result,
            last_watermark_ts=None,
        )

        logger.exception("Gold dim_subscriptions FAILED | run_id=%s", run_id)
        raise

    finally:
        if gold_df is not None:
            try:
                gold_df.unpersist()
            except Exception as e:
                logger.warning(
                    "Failed to unpersist gold_df: %s | run_id=%s",
                    e,
                    run_id,
                )