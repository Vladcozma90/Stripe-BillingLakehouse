from __future__ import annotations

import logging
import uuid

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
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

        "silver_current_table": f"{env.catalog}.{env.schemas['silver']}.s_current_erp_usage_daily",

        "gold_dim_customers_table": f"{env.catalog}.{env.schemas['gold']}.g_dim_customers",
        "gold_dim_plan_table": f"{env.catalog}.{env.schemas['gold']}.g_dim_plan_catalog",

        "gold_fact_table": f"{env.catalog}.{env.schemas['gold']}.g_fact_usage_daily",
        "gold_fact_path": f"{env.gold_base_path}/{env.catalog}/{env.schemas['gold']}/g_fact_usage_daily",
    }


def _get_required_usage_columns() -> list[str]:
    return [
        "usage_id",
        "event_ts",
        "usage_date",
        "account_id",
        "feature_code",
        "active_users",
        "units_raw",
        "source_system",
        "batch_id",
        "etl_run_id",
    ]


def _get_required_dim_customers_columns() -> list[str]:
    return [
        "customer_sk",
        "account_id",
        "stripe_customer_id",
        "plan_code",
        "account_silver_effective_start_ts",
        "account_silver_effective_end_ts",
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


def _build_stage_gold_fact_usage_daily(
    silver_df: DataFrame,
    dim_customers_df: DataFrame,
    dim_plan_df: DataFrame,
    run_id: str,
) -> DataFrame:
    stage_df = (
        silver_df
        .withColumn("usage_id", lower(trim(col("usage_id"))).cast("string"))
        .withColumn("event_ts", col("event_ts").cast("timestamp"))
        .withColumn("usage_date", col("usage_date").cast("date"))
        .withColumn("account_id", trim(col("account_id")).cast("string"))
        .withColumn("feature_code", lower(trim(col("feature_code"))).cast("string"))
        .withColumn("active_users", col("active_users").cast("bigint"))
        .withColumn("units_raw", col("units_raw").cast("bigint"))
        .withColumn("source_system", upper(trim(col("source_system"))).cast("string"))
        .withColumn("batch_id", trim(col("batch_id")).cast("string"))
    )

    customer_df = (
        dim_customers_df
        .select(
            col("customer_sk"),
            col("account_id").alias("dim_customer_account_id"),
            col("stripe_customer_id"),
            col("plan_code").alias("dim_customer_plan_code"),
            col("account_silver_effective_start_ts").alias("customers_effective_start_ts"),
            col("account_silver_effective_end_ts").alias("customers_effective_end_ts"),
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
            customer_df.alias("c"),
            (
                (col("f.account_id") == col("c.dim_customer_account_id"))
                & (col("f.event_ts") >= col("c.customers_effective_start_ts"))
                & (
                    (col("f.event_ts") < col("c.customers_effective_end_ts"))
                    | col("c.customers_effective_end_ts").isNull()
                )
            ),
            how="left",
        )
        .join(
            plan_df.alias("p"),
            (
                (col("c.dim_customer_plan_code") == col("p.dim_plan_code"))
                & (col("f.event_ts") >= col("p.plan_effective_start_ts"))
                & (
                    (col("f.event_ts") < col("p.plan_effective_end_ts"))
                    | col("p.plan_effective_end_ts").isNull()
                )
            ),
            how="left",
        )
    )

    fact_df = (
        joined_df
        .withColumn("usage_business_key", col("f.usage_id"))
        .withColumn("customer_sk", coalesce(col("c.customer_sk"), lit(-1)).cast("bigint"))
        .withColumn("plan_sk", coalesce(col("p.plan_sk"), lit(-1)).cast("bigint"))
        .withColumn("stripe_customer_id", col("c.stripe_customer_id").cast("string"))
        .withColumn("plan_code", col("c.dim_customer_plan_code").cast("string"))
        .withColumn("etl_run_id", lit(run_id).cast("string"))
        .withColumn("gold_processed_ts", current_timestamp())
        .withColumn("gold_processed_date", current_date())
        .select(
            "usage_business_key",
            col("f.usage_id").alias("usage_id"),

            "customer_sk",
            "plan_sk",

            col("f.account_id").alias("account_id"),
            "stripe_customer_id",
            "plan_code",

            col("f.event_ts").alias("event_ts"),
            col("f.usage_date").alias("usage_date"),
            col("f.feature_code").alias("feature_code"),
            col("f.active_users").alias("active_users"),
            col("f.units_raw").alias("units_raw"),
            col("f.source_system").alias("source_system"),
            col("f.batch_id").alias("batch_id"),

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
                    coalesce(col("usage_business_key").cast("string"), lit("")),
                    coalesce(col("usage_id").cast("string"), lit("")),
                    coalesce(col("customer_sk").cast("string"), lit("")),
                    coalesce(col("plan_sk").cast("string"), lit("")),
                    coalesce(col("account_id").cast("string"), lit("")),
                    coalesce(col("stripe_customer_id").cast("string"), lit("")),
                    coalesce(col("plan_code").cast("string"), lit("")),
                    coalesce(col("event_ts").cast("string"), lit("")),
                    coalesce(col("usage_date").cast("string"), lit("")),
                    coalesce(col("feature_code").cast("string"), lit("")),
                    coalesce(col("active_users").cast("string"), lit("")),
                    coalesce(col("units_raw").cast("string"), lit("")),
                    coalesce(col("source_system").cast("string"), lit("")),
                    coalesce(col("batch_id").cast("string"), lit("")),
                ),
                256,
            )
        )
    )


def _merge_gold_fact_usage_daily(
    spark: SparkSession,
    target_table: str,
    source_df: DataFrame,
) -> None:
    gold_dt = DeltaTable.forName(spark, target_table)

    merge_condition = "t.usage_business_key <=> s.usage_business_key"

    (
        gold_dt.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition="NOT (t.record_hash <=> s.record_hash)",
            set={
                "usage_id": "s.usage_id",
                "customer_sk": "s.customer_sk",
                "plan_sk": "s.plan_sk",
                "account_id": "s.account_id",
                "stripe_customer_id": "s.stripe_customer_id",
                "plan_code": "s.plan_code",
                "event_ts": "s.event_ts",
                "usage_date": "s.usage_date",
                "feature_code": "s.feature_code",
                "active_users": "s.active_users",
                "units_raw": "s.units_raw",
                "source_system": "s.source_system",
                "batch_id": "s.batch_id",
                "etl_run_id": "s.etl_run_id",
                "gold_processed_ts": "s.gold_processed_ts",
                "gold_processed_date": "s.gold_processed_date",
                "record_hash": "s.record_hash",
            },
        )
        .whenNotMatchedInsert(
            values={
                "usage_business_key": "s.usage_business_key",
                "usage_id": "s.usage_id",
                "customer_sk": "s.customer_sk",
                "plan_sk": "s.plan_sk",
                "account_id": "s.account_id",
                "stripe_customer_id": "s.stripe_customer_id",
                "plan_code": "s.plan_code",
                "event_ts": "s.event_ts",
                "usage_date": "s.usage_date",
                "feature_code": "s.feature_code",
                "active_users": "s.active_users",
                "units_raw": "s.units_raw",
                "source_system": "s.source_system",
                "batch_id": "s.batch_id",
                "etl_run_id": "s.etl_run_id",
                "gold_processed_ts": "s.gold_processed_ts",
                "gold_processed_date": "s.gold_processed_date",
                "record_hash": "s.record_hash",
            },
        )
        .execute()
    )


def run_gold_fact_usage_daily(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "gold_fact_usage_daily"
    dataset = "fact_usage_daily"
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
        logger.info("Gold fact_usage_daily start | run_id=%s", run_id)

        silver_df = spark.table(cfg["silver_current_table"])
        dim_customers_df = spark.table(cfg["gold_dim_customers_table"])
        dim_plan_df = spark.table(cfg["gold_dim_plan_table"])

        _validate_required_columns(
            df=silver_df,
            required_columns=_get_required_usage_columns(),
            table_name=cfg["silver_current_table"],
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
            logger.info("No data in silver current usage_daily. Exiting.")
            return

        rows_in = silver_df.count()

        gold_df = _build_stage_gold_fact_usage_daily(
            silver_df=silver_df,
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

            logger.info("No usage_daily rows to merge | run_id=%s", run_id)
            return

        _merge_gold_fact_usage_daily(
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
            "Gold fact_usage_daily SUCCESS | rows_in=%d | rows_out=%d",
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
        logger.exception("Gold fact_usage_daily FAILED | run_id=%s", run_id)
        raise

    finally:
        if gold_df is not None:
            try:
                gold_df.unpersist()
            except Exception as e:
                logger.warning("Failed to unpersist gold_df: %s | run_id=%s", e, run_id)