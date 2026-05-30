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

        "silver_current_table": f"{env.catalog}.{env.schemas['silver']}.s_current_stripe_subscription_items",

        "gold_dim_subscriptions_table": f"{env.catalog}.{env.schemas['gold']}.g_dim_subscriptions",
        "gold_dim_customers_table": f"{env.catalog}.{env.schemas['gold']}.g_dim_customers",
        "gold_dim_plan_table": f"{env.catalog}.{env.schemas['gold']}.g_dim_plan_catalog",

        "gold_fact_table": f"{env.catalog}.{env.schemas['gold']}.g_fact_subscription_items",
        "gold_fact_path": f"{env.gold_base_path}/{env.catalog}/{env.schemas['gold']}/g_fact_subscription_items",
    }


def _get_required_subscription_item_columns() -> list[str]:
    return [
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


def _build_stage_gold_fact_subscription_items(
    silver_df: DataFrame,
    dim_subscriptions_df: DataFrame,
    dim_customers_df: DataFrame,
    dim_plan_df: DataFrame,
    run_id: str,
) -> DataFrame:
    stage_df = (
        silver_df
        .withColumn("subscription_item_id", trim(col("subscription_item_id")).cast("string"))
        .withColumn("subscription_id", trim(col("subscription_id")).cast("string"))
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

    subscription_df = (
        dim_subscriptions_df
        .select(
            col("subscription_sk"),
            col("subscription_id").alias("dim_subscription_id"),
            col("stripe_customer_id").alias("dim_subscription_stripe_customer_id"),
            col("silver_effective_start_ts").alias("subscriptions_effective_start_ts"),
            col("silver_effective_end_ts").alias("subscriptions_effective_end_ts"),
        )
    )

    customer_df = (
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
                & (col("f.item_created_ts") >= col("s.subscriptions_effective_start_ts"))
                & (
                    (col("f.item_created_ts") < col("s.subscriptions_effective_end_ts"))
                    | col("s.subscriptions_effective_end_ts").isNull()
                )
            ),
            how="left",
        )
        .join(
            customer_df.alias("c"),
            (
                (col("s.dim_subscription_stripe_customer_id") == col("c.dim_customer_stripe_customer_id"))
                & (col("f.item_created_ts") >= col("c.customers_effective_start_ts"))
                & (
                    (col("f.item_created_ts") < col("c.customers_effective_end_ts"))
                    | col("c.customers_effective_end_ts").isNull()
                )
            ),
            how="left",
        )
        .join(
            plan_df.alias("p"),
            (
                (col("c.dim_customer_plan_code") == col("p.dim_plan_code"))
                & (col("f.item_created_ts") >= col("p.plan_effective_start_ts"))
                & (
                    (col("f.item_created_ts") < col("p.plan_effective_end_ts"))
                    | col("p.plan_effective_end_ts").isNull()
                )
            ),
            how="left",
        )
    )

    fact_df = (
        joined_df
        .withColumn("subscription_item_business_key", col("f.subscription_item_id"))
        .withColumn("subscription_sk", coalesce(col("s.subscription_sk"), lit(-1)).cast("bigint"))
        .withColumn("customer_sk", coalesce(col("c.customer_sk"), lit(-1)).cast("bigint"))
        .withColumn("plan_sk", coalesce(col("p.plan_sk"), lit(-1)).cast("bigint"))
        .withColumn("stripe_customer_id", col("s.dim_subscription_stripe_customer_id").cast("string"))
        .withColumn("account_id", col("c.account_id").cast("string"))
        .withColumn("plan_code", col("c.dim_customer_plan_code").cast("string"))
        .withColumn("etl_run_id", lit(run_id).cast("string"))
        .withColumn("gold_processed_ts", current_timestamp())
        .withColumn("gold_processed_date", current_date())
        .select(
            "subscription_item_business_key",
            col("f.subscription_item_id").alias("subscription_item_id"),

            "subscription_sk",
            "customer_sk",
            "plan_sk",

            col("f.subscription_id").alias("subscription_id"),
            "stripe_customer_id",
            "account_id",
            "plan_code",

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
                    coalesce(col("subscription_item_business_key").cast("string"), lit("")),
                    coalesce(col("subscription_item_id").cast("string"), lit("")),
                    coalesce(col("subscription_sk").cast("string"), lit("")),
                    coalesce(col("customer_sk").cast("string"), lit("")),
                    coalesce(col("plan_sk").cast("string"), lit("")),
                    coalesce(col("subscription_id").cast("string"), lit("")),
                    coalesce(col("stripe_customer_id").cast("string"), lit("")),
                    coalesce(col("account_id").cast("string"), lit("")),
                    coalesce(col("plan_code").cast("string"), lit("")),
                    coalesce(col("price_id").cast("string"), lit("")),
                    coalesce(col("product_id").cast("string"), lit("")),
                    coalesce(col("item_currency").cast("string"), lit("")),
                    coalesce(col("billing_interval").cast("string"), lit("")),
                    coalesce(col("price_type").cast("string"), lit("")),
                    coalesce(col("usage_type").cast("string"), lit("")),
                    coalesce(col("quantity").cast("string"), lit("")),
                    coalesce(col("unit_amount").cast("string"), lit("")),
                    coalesce(col("item_created_ts").cast("string"), lit("")),
                    coalesce(col("item_current_period_start_ts").cast("string"), lit("")),
                    coalesce(col("item_current_period_end_ts").cast("string"), lit("")),
                ),
                256,
            )
        )
    )


def _merge_gold_fact_subscription_items(
    spark: SparkSession,
    target_table: str,
    source_df: DataFrame,
) -> None:
    gold_dt = DeltaTable.forName(spark, target_table)

    merge_condition = "t.subscription_item_business_key <=> s.subscription_item_business_key"

    (
        gold_dt.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition="NOT (t.record_hash <=> s.record_hash)",
            set={
                "subscription_item_id": "s.subscription_item_id",
                "subscription_sk": "s.subscription_sk",
                "customer_sk": "s.customer_sk",
                "plan_sk": "s.plan_sk",
                "subscription_id": "s.subscription_id",
                "stripe_customer_id": "s.stripe_customer_id",
                "account_id": "s.account_id",
                "plan_code": "s.plan_code",
                "price_id": "s.price_id",
                "product_id": "s.product_id",
                "item_currency": "s.item_currency",
                "billing_interval": "s.billing_interval",
                "price_type": "s.price_type",
                "usage_type": "s.usage_type",
                "quantity": "s.quantity",
                "unit_amount": "s.unit_amount",
                "item_created_ts": "s.item_created_ts",
                "item_current_period_start_ts": "s.item_current_period_start_ts",
                "item_current_period_end_ts": "s.item_current_period_end_ts",
                "api_extracted_ts": "s.api_extracted_ts",
                "etl_run_id": "s.etl_run_id",
                "gold_processed_ts": "s.gold_processed_ts",
                "gold_processed_date": "s.gold_processed_date",
                "record_hash": "s.record_hash",
            },
        )
        .whenNotMatchedInsert(
            values={
                "subscription_item_business_key": "s.subscription_item_business_key",
                "subscription_item_id": "s.subscription_item_id",
                "subscription_sk": "s.subscription_sk",
                "customer_sk": "s.customer_sk",
                "plan_sk": "s.plan_sk",
                "subscription_id": "s.subscription_id",
                "stripe_customer_id": "s.stripe_customer_id",
                "account_id": "s.account_id",
                "plan_code": "s.plan_code",
                "price_id": "s.price_id",
                "product_id": "s.product_id",
                "item_currency": "s.item_currency",
                "billing_interval": "s.billing_interval",
                "price_type": "s.price_type",
                "usage_type": "s.usage_type",
                "quantity": "s.quantity",
                "unit_amount": "s.unit_amount",
                "item_created_ts": "s.item_created_ts",
                "item_current_period_start_ts": "s.item_current_period_start_ts",
                "item_current_period_end_ts": "s.item_current_period_end_ts",
                "api_extracted_ts": "s.api_extracted_ts",
                "etl_run_id": "s.etl_run_id",
                "gold_processed_ts": "s.gold_processed_ts",
                "gold_processed_date": "s.gold_processed_date",
                "record_hash": "s.record_hash",
            },
        )
        .execute()
    )


def run_gold_fact_subscription_items(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "gold_fact_stripe_subscription_items"
    dataset = "fact_stripe_subscription_items"
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
        target_table=cfg["gold_fact_table"],
        run_id=run_id,
    )

    try:
        logger.info("Gold fact_stripe_subscription_items start | run_id=%s", run_id)

        silver_df = spark.table(cfg["silver_current_table"])
        dim_subscriptions_df = spark.table(cfg["gold_dim_subscriptions_table"])
        dim_customers_df = spark.table(cfg["gold_dim_customers_table"])
        dim_plan_df = spark.table(cfg["gold_dim_plan_table"])

        _validate_required_columns(
            df=silver_df,
            required_columns=_get_required_subscription_item_columns(),
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
            logger.info("No data in silver current subscription items. Exiting.")
            return

        rows_in = silver_df.count()

        gold_df = _build_stage_gold_fact_subscription_items(
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

            logger.info("No subscription item rows to merge | run_id=%s", run_id)
            return

        _merge_gold_fact_subscription_items(
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
            "Gold fact_stripe_subscription_items SUCCESS | rows_in=%d | rows_out=%d",
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

        logger.exception("Gold fact_stripe_subscription_items FAILED | run_id=%s", run_id)
        raise

    finally:
        if gold_df is not None:
            try:
                gold_df.unpersist()
            except Exception as e:
                logger.warning("Failed to unpersist gold_df: %s | run_id=%s", e, run_id)