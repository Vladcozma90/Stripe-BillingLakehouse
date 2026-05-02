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

        "silver_current_table": f"{env.catalog}.{env.project}_silver.s_current_stripe_invoices",

        "gold_dim_subscription_table": f"{env.catalog}.{env.project}_gold.g_dim_subscription",
        "gold_dim_customer_table": f"{env.catalog}.{env.project}_gold.g_dim_customer",
        "gold_dim_account_table": f"{env.catalog}.{env.project}_gold.g_dim_account",
        "gold_dim_plan_table": f"{env.catalog}.{env.project}_gold.g_dim_plan",

        "gold_fact_table": f"{env.catalog}.{env.project}_gold.g_fact_invoice",
        "gold_fact_path": f"{env.gold_base_path}/{env.catalog}/{env.project}/stripe_invoices/g_fact_invoice",
    }

def _get_required_columns() -> list[str]:
    return [
        "invoice_id",
        "stripe_customer_id",
        "subscription_id",
        "invoice_status",
        "collection_method",
        "currency",
        "invoice_number",
        "amount_due",
        "amount_paid",
        "amount_remaining",
        "subtotal",
        "subtotal_excluding_tax",
        "total",
        "attempt_count",
        "is_attempted",
        "is_livemode",
        "auto_advance",
        "created_ts",
        "due_date_ts",
        "period_start_ts",
        "period_end_ts",
        "status_finalized_ts",
        "status_paid_ts",
        "status_voided_ts",
        "status_marked_uncollectible_ts",
        "api_extracted_ts",
        "_file_name",
        "_source",
        "_landing_format",
        "etl_run_id",
    ]


def _build_stage_gold_fact_invoices(
        silver_df: DataFrame,
        dim_subscription_df: DataFrame,
        dim_customer_df: DataFrame,
        dim_account_df: DataFrame,
        dim_plan_df: DataFrame,
) -> DataFrame:
    stage_df = (
        silver_df
        .withColumn("invoice_id", trim(col("invoice_id")).cast("string"))
        .withColumn("stripe_customer_id", trim(col("stripe_customer_id")).cast("string"))
        .withColumn("subscription_id", trim(col("subscription_id")).cast("string"))
        .withColumn("invoice_status", lower(trim(col("invoice_status"))).cast("string"))
        .withColumn("collection_method", lower(trim(col("collection_method"))).cast("string"))
        .withColumn("currency", lower(trim(col("currency"))).cast("string"))
        .withColumn("invoice_number", trim(col("invoice_number")).cast("string"))
        .withColumn("amount_due", col("amount_due").cast("bigint"))
        .withColumn("amount_paid", col("amount_paid").cast("bigint"))
        .withColumn("amount_remaining", col("amount_remaining").cast("bigint"))
        .withColumn("subtotal", col("subtotal").cast("bigint"))
        .withColumn("subtotal_excluding_tax", col("subtotal_excluding_tax").cast("bigint"))
        .withColumn("total", col("total").cast("bigint"))
        .withColumn("attempt_count", col("attempt_count").cast("bigint"))
        .withColumn("is_attempted", col("is_attempted").cast("boolean"))
        .withColumn("is_livemode", col("is_livemode").cast("boolean"))
        .withColumn("auto_advance", col("auto_advance").cast("boolean"))
        .withColumn("created_ts", col("created_ts").cast("timestamp"))
        .withColumn("due_date_ts", col("due_date_ts").cast("timestamp"))
        .withColumn("period_start_ts", col("period_start_ts").cast("timestamp"))
        .withColumn("period_end_ts", col("period_end_ts").cast("timestamp"))
        .withColumn("status_finalized_ts", col("status_finalized_ts").cast("timestamp"))
        .withColumn("status_paid_ts", col("status_paid_ts").cast("timestamp"))
        .withColumn("status_voided_ts", col("status_voided_ts").cast("timestamp"))
        .withColumn("status_marked_uncollectible_ts", col("status_marked_uncollectible_ts").cast("timestamp"))
        .withColumn("api_extracted_ts", col("api_extracted_ts").cast("timestamp"))
    )


    subscription_current_df = (
        dim_subscription_df
        .filter("is_current" == True)
        .select(
            col("stripe_subscription_sk"),
            col("subscription_id").alias("dim_subscription_id"),
            col("stripe_customer_id").alias("dim_customer_id"),
        )
    )

    customer_current_df = (
        dim_customer_df
        .filter("is_current" == True)
        .select(
            col("stripe_customers_sk"),
            col("customer_id"),
        )
    )

    account_current_df = (
        dim_account_df
        .filter("is_current" == True)
        .select(
            col("account_master_snapshot_sk"),
            col("stripe_customer_id").alias("dim_account_stripe_customer_id"),
            col("plan_code").alias("dim_account_plan_code"),
        )
    )

    plan_current_df = (
        dim_plan_df
        .filter("is_current" == True)
        .select(
            col("plan_catalog_sk"),
            col("plan_code").alias("dim_plan_code")
        )
    )

    joined_df = (
        stage_df.alias("f")
        .join(
            subscription_current_df.alias("s"),
            col("f.subscription_id") == col("s.dim_subscription_id"),
            how="left",
        )
        .join(
            customer_current_df.alias("c"),
            col("f.stripe_customer_id") == col("c.dim_customer_id"),
            how="left",
        )
        .join(
            account_current_df.alias("a"),
            col("f.stripe_customer_id") == col("a.dim_account_stripe_customer_id"),
            how="left",
        )
        .join(
            plan_current_df.alias("p"),
            col("a.dim_account_plan_code") == col("p.dim_plan_code"),
            how="left",
        )
    )

    return (
        joined_df
        .withColumn("stripe_subscriptions_sk", coalesce(col("s.stripe_subscriptions_sk"), lit(-1)))
        .withColumn("stripe_customers_sk", coalesce(col("c.stripe_customers_sk"), lit(-1)))
        .withColumn("account_master_snapshot_sk", coalesce(col("a.account_master_snapshot_sk"), lit(-1)))
        .withColumn("plan_catalog_sk", coalesce(col("p.plan_catalog_sk"), lit(-1)))
        .withColumn("plan_code", col("a.dim_account_plan_code"))
        .withColumn("gold_loaded_ts", current_timestamp())
        .withColumn("gold_loaded_date", current_date())
        .select(
            col("f.invoice_id").alias("invoice_id"),
            col("stripe_subscriptions_sk"),
            col("stripe_customers_sk"),
            col("account_master_snapshot_sk"),
            col("plan_catalog_sk"),
            col("f.stripe_customer_id").alias("stripe_customer_id"),
            col("f.subscription_id").alias("subscription_id"),
            col("plan_code"),
            col("f.invoice_status").alias("invoice_status"),
            col("f.collection_method").alias("collection_method"),
            col("f.currency").alias("currency"),
            col("f.invoice_number").alias("invoice_number"),
            col("f.amount_due").alias("amount_due"),
            col("f.amount_paid").alias("amount_paid"),
            col("f.amount_remaining").alias("amount_remaining"),
            col("f.subtotal").alias("subtotal"),
            col("f.subtotal_excluding_tax").alias("subtotal_excluding_tax"),
            col("f.total").alias("total"),
            col("f.attempt_count").alias("attempt_count"),
            col("f.is_attempted").alias("is_attempted"),
            col("f.is_livemode").alias("is_livemode"),
            col("f.auto_advance").alias("auto_advance"),
            col("f.created_ts").alias("created_ts"),
            col("f.due_date_ts").alias("due_date_ts"),
            col("f.period_start_ts").alias("period_start_ts"),
            col("f.period_end_ts").alias("period_end_ts"),
            col("f.status_finalized_ts").alias("status_finalized_ts"),
            col("f.status_paid_ts").alias("status_paid_ts"),
            col("f.status_voided_ts").alias("status_voided_ts"),
            col("f.status_marked_uncollectible_ts").alias("status_marked_uncollectible_ts"),
            col("f.api_extracted_ts").alias("api_extracted_ts"),
            col("f._file_name").alias("_file_name"),
            col("f._source").alias("_source"),
            col("f._landing_format").alias("_landing_format"),
            col("f.etl_run_id").alias("etl_run_id"),
            col("gold_loaded_ts"),
            col("gold_loaded_date"),
        )
    )


def run_gold_fact_invoices(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "gold_fact_stripe_invoices"
    dataset = "fact_stripe_invoices"
    run_id = uuid.uuid4().hex

    cfg = _build_config(env=env)

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

        logger.info("Gold fact_stripe_invoice start | run_id=%s ", run_id)

        silver_df = spark.read.table(cfg["silver_current_table"])
        dim_subscription_df = spark.table(cfg["gold_dim_subscription_table"])
        dim_customer_df = spark.table(cfg["gold_dim_customer_table"])
        dim_account_df = spark.table(cfg["gold_dim_account_table"])
        dim_plan_df = spark.table(cfg["gold_dim_plan_table"])

        required_columns = _get_required_columns()
        missing_columns = [c for c in required_columns if c not in silver_df.columns]

        if missing_columns:
            raise ValueError(f"silver current missing required cols: {missing_columns}")
        
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
        
        gold_df = _build_stage_gold_fact_invoices(
            silver_df=silver_df,
            dim_subscription_df=dim_subscription_df,
            dim_customer_df=dim_customer_df,
            dim_account_df=dim_account_df,
            dim_plan_df=dim_plan_df,
        )

        rows_in = silver_df.count()
        rows_out = gold_df.count()

        gold_dt = DeltaTable.forName(spark, cfg["gold_fact_table"])

        (
            gold_dt.alias("t")
            .merge(
                gold_df.alias("s"),
                "t.invoice_id = s.invoice_id",
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
            "Gold fact_invoice SUCCESS | rows_in=%d | rows_out=%d",
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
        logger.exception("Gold fact_invoice FAILED")
        raise