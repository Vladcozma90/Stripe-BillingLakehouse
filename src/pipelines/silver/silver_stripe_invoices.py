from __future__ import annotations

import uuid
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    lower,
    current_timestamp,
    current_date,
    expr,
    coalesce,
)

from src.services.envs import EnvConfig
from src.services.audit import (
    insert_run_log_start,
    update_run_log_no_new_data,
    update_run_log_success,
    update_run_log_failure,
)
from src.services.watermark import read_incremental_by_watermark, upsert_watermark
from src.services.delta_table import write_append_table
from src.services.snapshot import merge_current_snapshot
from src.services.dq import (
    evaluate_dq_rules,
    build_dq_results_df,
    build_dq_failure_message,
    quarantine_by_business_key,
)
from src.services.transformations import deduplicate_by_business_key


logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.schemas['ops']}.run_logs",
        "state_table": f"{env.catalog}.{env.schemas['ops']}.pipeline_state",
        "dq_table": f"{env.catalog}.{env.schemas['silver']}.s_dq_stripe_invoices",
        "quarantine_table": f"{env.catalog}.{env.schemas['silver']}.s_quarantine_stripe_invoices",
        "current_table": f"{env.catalog}.{env.schemas['silver']}.s_current_stripe_invoices",

        "bronze_path": f"{env.bronze_base_path}/{env.catalog}/{env.schemas['bronze']}/b_stripe_invoices",
        "dq_path": f"{env.silver_base_path}/{env.catalog}/{env.schemas['silver']}/s_stripe_invoices/s_dq_stripe_invoices",
        "quarantine_path": f"{env.silver_base_path}/{env.catalog}/{env.schemas['silver']}/s_stripe_invoices/s_quarantine_stripe_invoices",
        "current_path": f"{env.silver_base_path}/{env.catalog}/{env.schemas['silver']}/s_stripe_invoices/s_current_stripe_invoices",
    }


def _get_required_columns() -> list[str]:
    return [
        "_extracted_at",
        "data",
        "_ingest_ts",
        "_ingest_date",
        "_file_name",
        "_source",
        "_landing_format",
    ]


def _build_stage_stripe_invoices(incr_df: DataFrame, run_id: str) -> DataFrame:
    df = (
        incr_df
        .withColumn("invoice_id", col("data.id").cast("string"))
        .withColumn("stripe_customer_id", col("data.customer").cast("string"))
        .withColumn("subscription_id", col("data.subscription").cast("string"))
        .withColumn("invoice_status", lower(trim(col("data.status"))).cast("string"))
        .withColumn("collection_method", lower(trim(col("data.collection_method"))).cast("string"))
        .withColumn("currency", lower(trim(col("data.currency"))).cast("string"))
        .withColumn("invoice_number", col("data.number").cast("string"))
        .withColumn("amount_due", col("data.amount_due").cast("bigint"))
        .withColumn("amount_paid", col("data.amount_paid").cast("bigint"))
        .withColumn("amount_remaining", col("data.amount_remaining").cast("bigint"))
        .withColumn("subtotal", col("data.subtotal").cast("bigint"))
        .withColumn("subtotal_excluding_tax", col("data.subtotal_excluding_tax").cast("bigint"))
        .withColumn("total", col("data.total").cast("bigint"))
        .withColumn("attempt_count", col("data.attempt_count").cast("bigint"))
        .withColumn("is_attempted", col("data.attempted").cast("boolean"))
        .withColumn("is_livemode", col("data.livemode").cast("boolean"))
        .withColumn("auto_advance", col("data.auto_advance").cast("boolean"))

        .withColumn(
            "created_ts",
            expr("try_cast(from_unixtime(cast(data.created as bigint)) as timestamp)")
        )
        .withColumn(
            "due_date_ts",
            expr("try_cast(from_unixtime(cast(data.due_date as bigint)) as timestamp)")
        )
        .withColumn(
            "period_start_ts",
            expr("try_cast(from_unixtime(cast(data.period_start as bigint)) as timestamp)")
        )
        .withColumn(
            "period_end_ts",
            expr("try_cast(from_unixtime(cast(data.period_end as bigint)) as timestamp)")
        )

        .withColumn(
            "status_finalized_ts",
            expr("try_cast(from_unixtime(cast(data.status_transitions.finalized_at as bigint)) as timestamp)")
        )
        .withColumn(
            "status_paid_ts",
            expr("try_cast(from_unixtime(cast(data.status_transitions.paid_at as bigint)) as timestamp)")
        )
        .withColumn(
            "status_voided_ts",
            expr("try_cast(from_unixtime(cast(data.status_transitions.voided_at as bigint)) as timestamp)")
        )
        .withColumn(
            "status_marked_uncollectible_ts",
            expr("try_cast(from_unixtime(cast(data.status_transitions.marked_uncollectible_at as bigint)) as timestamp)")
        )

        .withColumn(
            "api_extracted_ts",
            coalesce(
                expr("try_cast(trim(_extracted_at) as timestamp)"),
                expr("try_to_timestamp(trim(_extracted_at), 'yyyy-MM-dd HH:mm:ss')"),
                expr("try_to_timestamp(trim(_extracted_at), 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX')"),
                expr("try_to_timestamp(trim(_extracted_at), 'yyyy-MM-dd''T''HH:mm:ssXXX')")
            )
        )
        .withColumn("etl_run_id", lit(run_id))
        .withColumn("silver_processed_ts", current_timestamp())
        .withColumn("silver_processed_date", current_date())
    )

    return df.select(
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
        "_ingest_ts",
        "_ingest_date",
        "_file_name",
        "_source",
        "_landing_format",
        "etl_run_id",
        "silver_processed_ts",
        "silver_processed_date",
    )


def run_silver_stripe_invoices(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "silver_stripe_invoices"
    dataset = "stripe_invoices"
    run_id = uuid.uuid4().hex

    cfg = _build_config(env)

    rows_in = 0
    rows_quarantined = 0
    rows_out = 0
    last_wm = None
    new_wm = None
    dq_result = "OK"

    business_key = ["invoice_id"]
    order_columns = ["_ingest_ts", "silver_processed_ts"]

    stage_df = None
    bad_records = None
    dedup_df = None

    insert_run_log_start(
        spark=spark,
        run_logs_table=cfg["run_logs_table"],
        pipeline_name=pipeline_name,
        dataset=dataset,
        target_table=cfg["current_table"],
        run_id=run_id,
    )

    try:
        logger.info("Silver stripe_invoices start | run_id=%s", run_id)

        bronze_df = spark.read.format("delta").load(cfg["bronze_path"])

        required_columns = _get_required_columns()
        missing_columns = [c for c in required_columns if c not in bronze_df.columns]

        if missing_columns:
            raise ValueError(f"Bronze missing required cols: {missing_columns}")

        incr_df, last_wm, new_wm = read_incremental_by_watermark(
            spark=spark,
            source_df=bronze_df,
            state_table=cfg["state_table"],
            pipeline_name=pipeline_name,
            dataset=dataset,
            watermark_col="_ingest_ts",
        )

        if incr_df.isEmpty():
            update_run_log_no_new_data(
                spark=spark,
                run_logs_table=cfg["run_logs_table"],
                pipeline_name=pipeline_name,
                dataset=dataset,
                run_id=run_id,
                last_watermark_ts=last_wm,
            )
            logger.info("No new data. Exiting.")
            return

        stage_df = _build_stage_stripe_invoices(
            incr_df=incr_df,
            run_id=run_id,
        ).persist()

        dq_rules = env.datasets[dataset]["data_quality"]["rules"]
        dq_metrics = evaluate_dq_rules(stage_df, dq_rules)
        dq_result = dq_metrics["overall_result"]

        dq_df = build_dq_results_df(
            spark=spark,
            dq_source="stage_stripe_invoices",
            run_id=run_id,
            metrics=dq_metrics,
        )

        write_append_table(
            spark=spark,
            df=dq_df,
            table_name=cfg["dq_table"],
            table_path=cfg["dq_path"],
        )

        if dq_result == "FAIL":
            raise ValueError(build_dq_failure_message(dq_metrics))

        bad_records, good_records = quarantine_by_business_key(
            stage_df=stage_df,
            key_columns=business_key,
        )

        bad_records = bad_records.persist()

        write_append_table(
            spark=spark,
            df=bad_records,
            table_name=cfg["quarantine_table"],
            table_path=cfg["quarantine_path"],
        )

        dedup_df = deduplicate_by_business_key(
            df=good_records,
            key_columns=business_key,
            order_columns=order_columns,
        ).persist()

        rows_in = stage_df.count()
        rows_out = dedup_df.count()
        rows_quarantined = bad_records.count()

        if rows_out == 0:
            upsert_watermark(
                spark=spark,
                state_table=cfg["state_table"],
                new_wm=new_wm,
                pipeline_name=pipeline_name,
                dataset=dataset,
                run_id=run_id,
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
                last_watermark_ts=last_wm,
            )

            logger.info(
                "No valid invoice rows to merge | run_id=%s",
                run_id,
            )
            return

        merge_current_snapshot(
            spark=spark,
            current_table=cfg["current_table"],
            df=dedup_df,
            key_columns=business_key,
        )

        upsert_watermark(
            spark=spark,
            state_table=cfg["state_table"],
            new_wm=new_wm,
            pipeline_name=pipeline_name,
            dataset=dataset,
            run_id=run_id,
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
            last_watermark_ts=last_wm,
        )

        logger.info(
            "Silver stripe_invoices SUCCESS | rows_in=%d | rows_out=%d | rows_quarantined=%d",
            rows_in,
            rows_out,
            rows_quarantined,
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
            last_watermark_ts=last_wm,
        )

        logger.exception("Silver stripe_invoices FAILED | run_id=%s", run_id)
        raise

    finally:
        for cached_df in (stage_df, bad_records, dedup_df):
            if cached_df is not None:
                try:
                    cached_df.unpersist()
                except Exception as e:
                    logger.warning("Failed to unpersist cached DataFrame: %s | run_id=%s", e, run_id)