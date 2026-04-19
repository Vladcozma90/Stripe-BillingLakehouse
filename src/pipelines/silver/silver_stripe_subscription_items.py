from __future__ import annotations
import uuid
import logging
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    lower,
    to_timestamp,
    from_unixtime,
    current_timestamp,
    current_date,
    sha2,
    concat_ws,
    coalesce
)

from services.envs import EnvConfig
from services.audit import (
    insert_run_log_start,
    update_run_log_no_new_data,
    update_run_log_success,
    update_run_log_failure
)
from services.watermark import read_incremental_by_watermark, upsert_watermark
from services.dq import evaluate_dq_rules, build_dq_results_df, build_dq_failure_message, quarantine_by_business_key
from services.delta_table import write_append_table
from services.transformations import deduplicate_by_business_key
from services.snapshot import merge_current_snapshot


logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table": f"{env.catalog}.{env.project}_ops.pipeline_state",
        "dq_table": f"{env.catalog}.{env.project}_silver.s_dq_stripe_subscription_items",
        "quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_stripe_subscription_items",
        "current_table": f"{env.catalog}.{env.project}_silver.s_current_stripe_subscription_items",

        "bronze_path": f"{env.raw_base_path}/{env.project}/b_stripe_subscription_items",
        "dq_path": f"{env.curated_base_path}/{env.project}/stripe_subscription_items/s_dq_stripe_subscription_items",
        "quarantine_path": f"{env.curated_base_path}/{env.project}/stripe_subscription_items/s_quarantine_stripe_subscription_items",
        "current_path": f"{env.curated_base_path}/{env.project}/stripe_subscription_items/s_current_stripe_subscription_items",
    }


def _get_required_columns() -> list[str]:
    return [
        "_extracted_at",
        "date",
        "_ingest_date",
        "_file_name",
        "_source",
        "_landing_format",
    ]


def _build_stage_stripe_subscription_items(incr_df: DataFrame, run_id: str) -> DataFrame:
    df = (
        incr_df
        # identifiers
        .withColumn("subscription_item_id", col("data.id").cast("string"))
        .withColumn("subscription_id", col("data.subscription").cast("string"))

        # pricing
        .withColumn("price_id", col("data.price.id").cast("string"))
        .withColumn("product_id", col("data.price.product").cast("string"))
        .withColumn("item_currency", lower(trim(col("data.price.currency"))).cast("string"))
        .withColumn("unit_amount", col("data.price.unit_amount").cast("bigint"))
        .withColumn("billing_interval", lower(trim(col("data.price.recurring.interval"))).cast("string"))
        .withColumn("price_type", lower(trim(col("data.price.type"))).cast("string"))
        .withColumn("usage_type", lower(trim(col("data.price.recurring.usage_type"))).cast("string"))

        # quantity
        .withColumn("quantity", col("data.quantity").cast("bigint"))

        # timestamps
        .withColumn("item_created_ts", to_timestamp(from_unixtime(col("data.created"))))
        .withColumn("item_current_period_start_ts", to_timestamp(from_unixtime(col("data.current_period_start"))))
        .withColumn("item_current_period_end_ts", to_timestamp(from_unixtime(col("data.current_period_end"))))

        # ingestion metadata
        .withColumn("api_extracted_ts", to_timestamp(col("_extracted_at")))
        .withColumn("etl_run_id", lit(run_id))
        .withColumn("silver_processed_ts", current_timestamp())
        .withColumn("silver_processed_date", current_date())
    )

    return df.select(
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
        "_ingest_ts",
        "_ingest_date",
        "_file_name",
        "_source",
        "_landing_format",
        "etl_run_id",
        "silver_processed_ts",
        "silver_processed_date",
    )


def run_silver_stripe_subscription_items(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "silver_stripe_subscription_items"
    dataset = "stripe_subscription_items"
    run_id = uuid.uuid4().hex

    cfg = _build_config(env)

    business_key = ["subscription_item_id"]
    order_columns = ["_ingest_ts", "silver_processed_ts"]

    insert_run_log_start(
        spark=spark,
        run_logs_table=cfg["run_logs_table"],
        pipeline_name=pipeline_name,
        dataset=dataset,
        target_table=cfg["current_table"],
        run_id=run_id
    )

    try:

        logger.info("Silver stripe_subscription_items start | run_id=%s", run_id)

        # Read bronze and validate data source
        bronze_df = spark.read.format("delta").load(cfg["bronze_path"])

        required_columns = _get_required_columns()

        missing_columns = [c for c in required_columns if c not in bronze_df.columns]

        if missing_columns:
            raise ValueError(f"missing required columns: {missing_columns}")
        
        incr_df, last_wm, new_wm = read_incremental_by_watermark(
            spark=spark,
            source_df=bronze_df,
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
                last_watermark_ts=last_wm
            )
            logger.info("No new data. Exiting.")
            return
        
        stage_df = _build_stage_stripe_subscription_items(
            incr_df=incr_df,
            run_id=run_id
        )

        # dq

        dq_rules = env.datasets[dataset]["data_quality"]["rules"]
        dq_metrics = evaluate_dq_rules(df=stage_df, rules=dq_rules)
        dq_result = dq_metrics["overall_result"]

        dq_df = build_dq_results_df(
            spark=spark,
            dq_source=stage_df,
            run_id=run_id,
            metrics=dq_metrics
        )

        write_append_table(
            spark=spark,
            df=dq_df,
            table_name=cfg["dq_table"],
            table_path=cfg["dq_path"],
        )

        if dq_result == "FAIL":
            raise ValueError(build_dq_failure_message(dq_metrics))
        
        # quarantination

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
        

        # deduplication

        dedup_df = deduplicate_by_business_key(
            df=good_records,
            key_columns=business_key,
            order_columns=order_columns,
        )

        rows_in = stage_df.count()
        rows_out = dedup_df.count()
        rows_quarantined = bad_records.count()


        # current creation

        merge_current_snapshot(
            spark=spark,
            current_table=cfg["current_table"],
            df=dedup_df,
            key_columns=business_key
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

        logger.info("Silver stripe_subscription_items | rows_in = %d | rows_out = %d | rows_quarantined =%d",
                    rows_in,
                    rows_out,
                    rows_quarantined
                    )

    except Exception as e:
        update_run_log_failure(
            spark=spark,
            run_logs_table=cfg["run_logs_table"],
            pipeline_name=pipeline_name,
            dataset=dataset,
            run_id=run_id,
            error_msg=e,
            rows_in=rows_in,
            rows_out=rows_out,
            rows_quarantined=rows_quarantined,
            dq_result=dq_result,
            last_watermark_ts=last_wm,
        )

        logger.exception("Silver stripe_subscription_items FAILED")
        raise