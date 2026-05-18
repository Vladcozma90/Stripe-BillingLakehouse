from __future__ import annotations

import uuid
import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    lower,
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
        "silver_conform_table": f"{env.catalog}.{env.schemas['silver']}.s_conform_erp_plan_catalog",
        "gold_table": f"{env.catalog}.{env.schemas['gold']}.dim_plan_catalog",

        "gold_path": f"{env.gold_base_path}/{env.catalog}/{env.schemas['gold']}/dim_plan_catalog",
    }


def _get_required_columns() -> list[str]:
    return [
        "plan_code",
        "plan_name",
        "monthly_price_usd",
        "seats_included",
        "max_units_per_month",
        "currency",
        "billing_period",
        "effective_from",
        "effective_to",
        "source_is_current",
        "price_version",
        "silver_effective_start_ts",
        "silver_effective_end_ts",
        "is_current",
    ]


def _build_gold_dim_plan_catalog(
    silver_conform_df: DataFrame,
    run_id: str,
) -> DataFrame:
    dim_df = (
        silver_conform_df
        .filter(col("is_current") == lit(True))
        .filter(col("plan_code") != lit("UNKNOWN"))
        .withColumn("plan_business_key", trim(col("plan_code")).cast("string"))
        .withColumn("plan_code", trim(col("plan_code")).cast("string"))
        .withColumn("plan_name", trim(col("plan_name")).cast("string"))
        .withColumn("monthly_price_usd", col("monthly_price_usd").cast("bigint"))
        .withColumn("seats_included", col("seats_included").cast("bigint"))
        .withColumn("max_units_per_month", col("max_units_per_month").cast("bigint"))
        .withColumn("currency", upper(trim(col("currency"))).cast("string"))
        .withColumn("billing_period", lower(trim(col("billing_period"))).cast("string"))
        .withColumn("effective_from", col("effective_from").cast("date"))
        .withColumn("effective_to", col("effective_to").cast("date"))
        .withColumn("source_is_current", col("source_is_current").cast("boolean"))
        .withColumn("price_version", trim(col("price_version")).cast("string"))
        .withColumn("silver_effective_start_ts", col("silver_effective_start_ts").cast("timestamp"))
        .withColumn("silver_effective_end_ts", col("silver_effective_end_ts").cast("timestamp"))
        .withColumn("etl_run_id", lit(run_id).cast("string"))
        .withColumn("gold_processed_ts", current_timestamp())
        .withColumn("gold_processed_date", current_date())
    )

    return (
        dim_df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("plan_business_key").cast("string"), lit("")),
                    coalesce(col("plan_code").cast("string"), lit("")),
                    coalesce(col("plan_name").cast("string"), lit("")),
                    coalesce(col("monthly_price_usd").cast("string"), lit("")),
                    coalesce(col("seats_included").cast("string"), lit("")),
                    coalesce(col("max_units_per_month").cast("string"), lit("")),
                    coalesce(col("currency").cast("string"), lit("")),
                    coalesce(col("billing_period").cast("string"), lit("")),
                    coalesce(col("effective_from").cast("string"), lit("")),
                    coalesce(col("effective_to").cast("string"), lit("")),
                    coalesce(col("source_is_current").cast("string"), lit("")),
                    coalesce(col("price_version").cast("string"), lit("")),
                ),
                256,
            )
        )
        .select(
            "plan_business_key",
            "plan_code",
            "plan_name",
            "monthly_price_usd",
            "seats_included",
            "max_units_per_month",
            "currency",
            "billing_period",
            "effective_from",
            "effective_to",
            "source_is_current",
            "price_version",
            "silver_effective_start_ts",
            "silver_effective_end_ts",
            "etl_run_id",
            "gold_processed_ts",
            "gold_processed_date",
            "record_hash",
        )
    )


def _merge_gold_dim_plan_catalog(
    spark: SparkSession,
    target_table: str,
    source_df: DataFrame,
) -> None:
    target_dt = DeltaTable.forName(spark, target_table)

    merge_condition = "t.plan_business_key <=> s.plan_business_key"

    (
        target_dt.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition="t.record_hash <> s.record_hash",
            set={
                "plan_code": "s.plan_code",
                "plan_name": "s.plan_name",
                "monthly_price_usd": "s.monthly_price_usd",
                "seats_included": "s.seats_included",
                "max_units_per_month": "s.max_units_per_month",
                "currency": "s.currency",
                "billing_period": "s.billing_period",
                "effective_from": "s.effective_from",
                "effective_to": "s.effective_to",
                "source_is_current": "s.source_is_current",
                "price_version": "s.price_version",
                "silver_effective_start_ts": "s.silver_effective_start_ts",
                "silver_effective_end_ts": "s.silver_effective_end_ts",
                "etl_run_id": "s.etl_run_id",
                "gold_processed_ts": "s.gold_processed_ts",
                "gold_processed_date": "s.gold_processed_date",
                "record_hash": "s.record_hash",
            },
        )
        .whenNotMatchedInsert(
            values={
                "plan_business_key": "s.plan_business_key",
                "plan_code": "s.plan_code",
                "plan_name": "s.plan_name",
                "monthly_price_usd": "s.monthly_price_usd",
                "seats_included": "s.seats_included",
                "max_units_per_month": "s.max_units_per_month",
                "currency": "s.currency",
                "billing_period": "s.billing_period",
                "effective_from": "s.effective_from",
                "effective_to": "s.effective_to",
                "source_is_current": "s.source_is_current",
                "price_version": "s.price_version",
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


def run_gold_dim_plan_catalog(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "gold_dim_plan_catalog"
    dataset = "dim_plan_catalog"
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
        logger.info("Gold dim_plan_catalog start | run_id=%s", run_id)

        silver_conform_df = spark.table(cfg["silver_conform_table"])

        required_columns = _get_required_columns()
        missing_columns = [c for c in required_columns if c not in silver_conform_df.columns]

        if missing_columns:
            raise ValueError(f"Silver conform missing required columns: {missing_columns}")

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

        rows_in = silver_conform_df.filter(col("is_current") == lit(True)).count()

        gold_df = _build_gold_dim_plan_catalog(
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

            logger.info("No current plan rows to merge | run_id=%s", run_id)
            return

        _merge_gold_dim_plan_catalog(
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
            "Gold dim_plan_catalog SUCCESS | rows_in=%d | rows_out=%d",
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

        logger.exception("Gold dim_plan_catalog FAILED | run_id=%s", run_id)
        raise

    finally:
        if gold_df is not None:
            try:
                gold_df.unpersist()
            except Exception as e:
                logger.warning("Failed to unpersist gold_df: %s | run_id=%s", e, run_id)