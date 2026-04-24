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
    to_timestamp,
    from_unixtime,
)

from services.envs import EnvConfig
from services.audit import (
    insert_run_log_start,
    update_run_log_no_new_data,
    update_run_log_success,
    update_run_log_failure,
)
from services.watermark import read_incremental_by_watermark, upsert_watermark
from services.delta_table import write_append_table
from services.snapshot import merge_current_snapshot
from services.dq import (
    evaluate_dq_rules,
    build_dq_results_df,
    build_dq_failure_message,
    quarantine_by_business_key,
)
from services.transformations import deduplicate_by_business_key


logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table": f"{env.catalog}.{env.project}_ops.pipeline_state",
        "dq_table": f"{env.catalog}.{env.project}_silver.s_dq_stripe_invoices",
        "quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_stripe_invoices",
        "current_table": f"{env.catalog}.{env.project}_silver.s_current_stripe_invoices",

        "bronze_path": f"{env.raw_base_path}/{env.project}/stripe_invoices",
        "dq_path": f"{env.curated_base_path}/{env.project}/stripe_invoices/s_dq_stripe_invoices",
        "quarantine_path": f"{env.curated_base_path}/{env.project}/stripe_invoices/s_quarantine_stripe_invoices",
        "current_path": f"{env.curated_base_path}/{env.project}/stripe_invoices/s_current_stripe_invoices",
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

        .withColumn("created_ts", to_timestamp(from_unixtime(col("data.created"))))
        .withColumn("due_date_ts", to_timestamp(from_unixtime(col("data.due_date"))))
        .withColumn("period_start_ts", to_timestamp(from_unixtime(col("data.period_start"))))
        .withColumn("period_end_ts", to_timestamp(from_unixtime(col("data.period_end"))))
        
        .withColumn("status_finalized_ts", to_timestamp(from_unixtime(col("data.status_transitions.finalized_at"))))
        .withColumn("status_paid_ts", to_timestamp(from_unixtime(col("data.status_transitions.paid_at"))))
        .withColumn("status_voided_ts", to_timestamp(from_unixtime(col("data.status_transitions.voided_at"))))
        .withColumn("status_marked_uncollectible_ts", to_timestamp(from_unixtime(col("data.status_transitions.marked_uncollectible_at"))))

        .withColumn("api_extracted_ts", to_timestamp(col("_extracted_at")))
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
        )

        dq_rules = env.datasets[dataset]["data_quality"]["rules"]
        dq_metrics = evaluate_dq_rules(stage_df, dq_rules)
        dq_result = dq_metrics["overall_result"]

        dq_result_df = build_dq_results_df(
            spark=spark,
            table_name="stage_stripe_invoices",
            run_id=run_id,
            metrics=dq_metrics,
        )

        write_append_table(
            spark=spark,
            df=dq_result_df,
            table_name=cfg["dq_table"],
            table_path=cfg["dq_path"],
        )

        if dq_result == "FAIL":
            raise ValueError(build_dq_failure_message(dq_metrics))

        bad_records, good_records = quarantine_by_business_key(
            stage_df=stage_df,
            key_columns=business_key,
        )

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
        )

        rows_in = good_records.count()
        rows_out = dedup_df.count()
        rows_quarantined = bad_records.count()

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
            last_watermark_ts=new_wm,
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
            dq_result="ERROR",
            last_watermark_ts=last_wm,
        )
        logger.exception("Silver stripe_invoices FAILED")
        raise