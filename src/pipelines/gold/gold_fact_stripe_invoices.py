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
    sha2,
    concat_ws,
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

        "silver_current_table": f"{env.catalog}.{env.schemas['silver']}.s_current_stripe_invoices",

        "gold_dim_stripe_subscriptions_table": f"{env.catalog}.{env.schemas['gold']}.g_dim_stripe_subscriptions",
        "gold_dim_customers_table": f"{env.catalog}.{env.schemas['gold']}.g_dim_customers",
        "gold_dim_plan_table": f"{env.catalog}.{env.schemas['gold']}.g_dim_plan_catalog",

        "gold_fact_table": f"{env.catalog}.{env.schemas['gold']}.g_fact_stripe_invoices",
    }


def _get_required_invoice_columns() -> list[str]:
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
        "etl_run_id",
    ]


def _get_required_dim_subscriptions_columns() -> list[str]:
    return [
        "subscription_sk",
        "subscription_id",
        "stripe_customer_id",
        "silver_effective_start_ts",
        "silver_effective_end_ts",
    ]


def _get_required_dim_customers_columns() -> list[str]:
    return [
        "customer_sk",
        "account_id",
        "stripe_customer_id",
        "plan_code",
        "stripe_silver_effective_start_ts",
        "stripe_silver_effective_end_ts",
    ]


def _get_required_dim_plan_columns() -> list[str]:
    return [
        "plan_sk",
        "plan_code",
        "silver_effective_start_ts",
        "silver_effective_end_ts",
    ]


def _validate_required_columns(
    df: DataFrame,
    required_columns: list[str],
    table_name: str,
) -> None:
    missing_columns = [c for c in required_columns if c not in df.columns]

    if missing_columns:
        raise ValueError(f"{table_name} missing required columns: {missing_columns}")


def _build_stage_gold_fact_invoices(
    silver_df: DataFrame,
    dim_subscriptions_df: DataFrame,
    dim_customers_df: DataFrame,
    dim_plan_df: DataFrame,
    run_id: str,
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

    subscription_df = (
        dim_subscriptions_df
        .select(
            col("subscription_sk"),
            col("subscription_id").alias("dim_subscription_id"),
            col("stripe_customer_id").alias("dim_subscription_stripe_customer_id"),
            col("silver_effective_start_ts").alias("subscription_effective_start_ts"),
            col("silver_effective_end_ts").alias("subscription_effective_end_ts"),
        )
    )

    customers_df = (
        dim_customers_df
        .select(
            col("customer_sk"),
            col("stripe_customer_id").alias("dim_customer_stripe_customer_id"),
            col("account_id"),
            col("plan_code").alias("dim_customer_plan_code"),
            col("stripe_silver_effective_start_ts").alias("customers_effective_start_ts"),
            col("stripe_silver_effective_end_ts").alias("customers_effective_end_ts"),
        )
    )

    plan_df = (
        dim_plan_df
        .select(
            col("plan_sk"),
            col("plan_code").alias("dim_plan_code"),
            col("silver_effective_start_ts").alias("plan_effective_start_ts"),
            col("silver_effective_end_ts").alias("plan_effective_end_ts"),
        )
    )

    joined_df = (
        stage_df.alias("f")
        .join(
            subscription_df.alias("s"),
            (
                (col("f.subscription_id") == col("s.dim_subscription_id"))
                & (col("f.created_ts") >= col("s.subscription_effective_start_ts"))
                & (
                    (col("f.created_ts") < col("s.subscription_effective_end_ts"))
                    | col("s.subscription_effective_end_ts").isNull()
                )
            ),
            how="left",
        )
        .join(
            customers_df.alias("c"),
            (
                (col("f.stripe_customer_id") == col("c.dim_customer_stripe_customer_id"))
                & (col("f.created_ts") >= col("c.customers_effective_start_ts"))
                & (
                    (col("f.created_ts") < col("c.customers_effective_end_ts"))
                    | col("c.customers_effective_end_ts").isNull()
                )
            ),
            how="left",
        )
        .join(
            plan_df.alias("p"),
            (
                (col("c.dim_customer_plan_code") == col("p.dim_plan_code"))
                & (col("f.created_ts") >= col("p.plan_effective_start_ts"))
                & (
                    (col("f.created_ts") < col("p.plan_effective_end_ts"))
                    | col("p.plan_effective_end_ts").isNull()
                )
            ),
            how="left",
        )
    )

    fact_df = (
        joined_df
        .withColumn("invoice_business_key", col("f.invoice_id"))
        .withColumn("subscription_sk", coalesce(col("s.subscription_sk"), lit(-1)).cast("bigint"))
        .withColumn("customer_sk", coalesce(col("c.customer_sk"), lit(-1)).cast("bigint"))
        .withColumn("plan_sk", coalesce(col("p.plan_sk"), lit(-1)).cast("bigint"))
        .withColumn("account_id", col("c.account_id").cast("string"))
        .withColumn("plan_code", col("c.dim_customer_plan_code").cast("string"))
        .withColumn("etl_run_id", lit(run_id).cast("string"))
        .withColumn("gold_processed_ts", current_timestamp())
        .withColumn("gold_processed_date", current_date())
        .select(
            "invoice_business_key",
            col("f.invoice_id").alias("invoice_id"),

            "subscription_sk",
            "customer_sk",
            "plan_sk",

            col("f.stripe_customer_id").alias("stripe_customer_id"),
            col("f.subscription_id").alias("subscription_id"),
            "account_id",
            "plan_code",

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

            "etl_run_id",
            "gold_processed_ts",
            "gold_processed_date",
        )
    )

    return (
        fact_df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("invoice_business_key").cast("string"), lit("")),
                    coalesce(col("invoice_id").cast("string"), lit("")),
                    coalesce(col("subscription_sk").cast("string"), lit("")),
                    coalesce(col("customer_sk").cast("string"), lit("")),
                    coalesce(col("plan_sk").cast("string"), lit("")),
                    coalesce(col("stripe_customer_id").cast("string"), lit("")),
                    coalesce(col("subscription_id").cast("string"), lit("")),
                    coalesce(col("account_id").cast("string"), lit("")),
                    coalesce(col("plan_code").cast("string"), lit("")),
                    coalesce(col("invoice_status").cast("string"), lit("")),
                    coalesce(col("collection_method").cast("string"), lit("")),
                    coalesce(col("currency").cast("string"), lit("")),
                    coalesce(col("invoice_number").cast("string"), lit("")),
                    coalesce(col("amount_due").cast("string"), lit("")),
                    coalesce(col("amount_paid").cast("string"), lit("")),
                    coalesce(col("amount_remaining").cast("string"), lit("")),
                    coalesce(col("subtotal").cast("string"), lit("")),
                    coalesce(col("subtotal_excluding_tax").cast("string"), lit("")),
                    coalesce(col("total").cast("string"), lit("")),
                    coalesce(col("attempt_count").cast("string"), lit("")),
                    coalesce(col("is_attempted").cast("string"), lit("")),
                    coalesce(col("is_livemode").cast("string"), lit("")),
                    coalesce(col("auto_advance").cast("string"), lit("")),
                    coalesce(col("created_ts").cast("string"), lit("")),
                    coalesce(col("due_date_ts").cast("string"), lit("")),
                    coalesce(col("period_start_ts").cast("string"), lit("")),
                    coalesce(col("period_end_ts").cast("string"), lit("")),
                    coalesce(col("status_finalized_ts").cast("string"), lit("")),
                    coalesce(col("status_paid_ts").cast("string"), lit("")),
                    coalesce(col("status_voided_ts").cast("string"), lit("")),
                    coalesce(col("status_marked_uncollectible_ts").cast("string"), lit("")),
                ),
                256,
            )
        )
    )


def _merge_gold_fact_invoices(
    spark: SparkSession,
    target_table: str,
    source_df: DataFrame,
) -> None:
    gold_dt = DeltaTable.forName(spark, target_table)

    merge_condition = "t.invoice_business_key <=> s.invoice_business_key"

    (
        gold_dt.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition="NOT (t.record_hash <=> s.record_hash)",
            set={
                "invoice_id": "s.invoice_id",
                "subscription_sk": "s.subscription_sk",
                "customer_sk": "s.customer_sk",
                "plan_sk": "s.plan_sk",
                "stripe_customer_id": "s.stripe_customer_id",
                "subscription_id": "s.subscription_id",
                "account_id": "s.account_id",
                "plan_code": "s.plan_code",
                "invoice_status": "s.invoice_status",
                "collection_method": "s.collection_method",
                "currency": "s.currency",
                "invoice_number": "s.invoice_number",
                "amount_due": "s.amount_due",
                "amount_paid": "s.amount_paid",
                "amount_remaining": "s.amount_remaining",
                "subtotal": "s.subtotal",
                "subtotal_excluding_tax": "s.subtotal_excluding_tax",
                "total": "s.total",
                "attempt_count": "s.attempt_count",
                "is_attempted": "s.is_attempted",
                "is_livemode": "s.is_livemode",
                "auto_advance": "s.auto_advance",
                "created_ts": "s.created_ts",
                "due_date_ts": "s.due_date_ts",
                "period_start_ts": "s.period_start_ts",
                "period_end_ts": "s.period_end_ts",
                "status_finalized_ts": "s.status_finalized_ts",
                "status_paid_ts": "s.status_paid_ts",
                "status_voided_ts": "s.status_voided_ts",
                "status_marked_uncollectible_ts": "s.status_marked_uncollectible_ts",
                "api_extracted_ts": "s.api_extracted_ts",
                "etl_run_id": "s.etl_run_id",
                "gold_processed_ts": "s.gold_processed_ts",
                "gold_processed_date": "s.gold_processed_date",
                "record_hash": "s.record_hash",
            },
        )
        .whenNotMatchedInsert(
            values={
                "invoice_business_key": "s.invoice_business_key",
                "invoice_id": "s.invoice_id",
                "subscription_sk": "s.subscription_sk",
                "customer_sk": "s.customer_sk",
                "plan_sk": "s.plan_sk",
                "stripe_customer_id": "s.stripe_customer_id",
                "subscription_id": "s.subscription_id",
                "account_id": "s.account_id",
                "plan_code": "s.plan_code",
                "invoice_status": "s.invoice_status",
                "collection_method": "s.collection_method",
                "currency": "s.currency",
                "invoice_number": "s.invoice_number",
                "amount_due": "s.amount_due",
                "amount_paid": "s.amount_paid",
                "amount_remaining": "s.amount_remaining",
                "subtotal": "s.subtotal",
                "subtotal_excluding_tax": "s.subtotal_excluding_tax",
                "total": "s.total",
                "attempt_count": "s.attempt_count",
                "is_attempted": "s.is_attempted",
                "is_livemode": "s.is_livemode",
                "auto_advance": "s.auto_advance",
                "created_ts": "s.created_ts",
                "due_date_ts": "s.due_date_ts",
                "period_start_ts": "s.period_start_ts",
                "period_end_ts": "s.period_end_ts",
                "status_finalized_ts": "s.status_finalized_ts",
                "status_paid_ts": "s.status_paid_ts",
                "status_voided_ts": "s.status_voided_ts",
                "status_marked_uncollectible_ts": "s.status_marked_uncollectible_ts",
                "api_extracted_ts": "s.api_extracted_ts",
                "etl_run_id": "s.etl_run_id",
                "gold_processed_ts": "s.gold_processed_ts",
                "gold_processed_date": "s.gold_processed_date",
                "record_hash": "s.record_hash",
            },
        )
        .execute()
    )


def run_gold_fact_stripe_invoices(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "gold_fact_stripe_invoices"
    dataset = "fact_stripe_invoices"
    run_id = uuid.uuid4().hex

    cfg = _build_config(env=env)

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
        target_table=cfg["gold_fact_table"],
        run_id=run_id,
    )

    try:
        logger.info("Gold fact_stripe_invoices start | run_id=%s", run_id)

        silver_df = spark.table(cfg["silver_current_table"])
        dim_subscriptions_df = spark.table(cfg["gold_dim_subscriptions_table"])
        dim_customers_df = spark.table(cfg["gold_dim_customers_table"])
        dim_plan_df = spark.table(cfg["gold_dim_plan_table"])

        _validate_required_columns(
            df=silver_df,
            required_columns=_get_required_invoice_columns(),
            table_name=cfg["silver_current_table"],
        )

        _validate_required_columns(
            df=dim_subscriptions_df,
            required_columns=_get_required_dim_subscriptions_columns(),
            table_name=cfg["gold_dim_subscriptions_table"],
        )

        _validate_required_columns(
            df=dim_customers_df,
            required_columns=_get_required_dim_customers_columns(),
            table_name=cfg["gold_dim_customers_table"],
        )

        _validate_required_columns(
            df=dim_plan_df,
            required_columns=_get_required_dim_plan_columns(),
            table_name=cfg["gold_dim_plan_table"],
        )

        if silver_df.isEmpty():
            update_run_log_no_new_data(
                spark=spark,
                run_logs_table=cfg["run_logs_table"],
                pipeline_name=pipeline_name,
                dataset=dataset,
                run_id=run_id,
                last_watermark_ts=None,
            )

            logger.info("No data in silver current invoices. Exiting.")
            return

        rows_in = silver_df.count()

        gold_df = _build_stage_gold_fact_invoices(
            silver_df=silver_df,
            dim_subscriptions_df=dim_subscriptions_df,
            dim_customers_df=dim_customers_df,
            dim_plan_df=dim_plan_df,
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

            logger.info("No invoice rows to merge | run_id=%s", run_id)
            return

        _merge_gold_fact_invoices(
            spark=spark,
            target_table=cfg["gold_fact_table"],
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
            "Gold fact_stripe_invoices SUCCESS | rows_in=%d | rows_out=%d",
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

        logger.exception("Gold fact_stripe_invoices FAILED | run_id=%s", run_id)
        raise

    finally:
        if gold_df is not None:
            try:
                gold_df.unpersist()
            except Exception as e:
                logger.warning("Failed to unpersist gold_df: %s | run_id=%s", e, run_id)