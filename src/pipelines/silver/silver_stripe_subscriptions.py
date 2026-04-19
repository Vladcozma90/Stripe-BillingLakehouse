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

def _build_config(spark: SparkSession, env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table": f"{env.catalog}.{env.project}_ops.pipeline_state",
        "dq_table": f"{env.catalog}.{env.project}_silver.s_dq_stripe_subscriptions",
        "quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_stripe_subscriptions",
        "current_table": f"{env.catalog}.{env.project}_silver.s_current_stripe_subscriptions",
        "conform_table": f"{env.catalog}.{env.project}_silver.s_conform_stripe_subscriptions",

        "bronze_path": f"{env.catalog}/{env.project}/b_stripe_subscriptions",
        "dq_path": f"{env.catalog}/{env.project}/stripe_subscriptions/s_dq_stripe_subscriptions",
        "quarantine_path": f"{env.catalog}/{env.project}/stripe_subscriptions/s_quarantine_stripe_subscriptions",
        "current_path": f"{env.catalog}/{env.project}/stripe_subscriptions/s_current_stripe_subscriptions",
        "conform_path": f"{env.catalog}/{env.project}/stripe_subscriptions/s_conform_stripe_subscriptions",
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

def _build_stage_stripe_subscriptions(incr_df: DataFrame, run_id: str) -> DataFrame:
    df = (
        incr_df
        .withColumn("subscription_id", lower(col("data.id")).cast("string"))
        .withColumn("stripe_customer_id", lower(col("data.customer")).cast("string"))
        .withColumn("subscription_status", lower(col("data.status")).cast("string"))
        .withColumn("collection_method", lower(col("collection_method")).cast("string"))
        .withColumn("currency", lower(trim(col("data.currency"))).cast("string"))
        .withColumn("latest_invoice_id", col("data.latest_invoice").cast("string"))

        .withColumn("created_ts", to_timestamp(from_unixtime(col("data.created"))))
        .withColumn("start_date_ts", to_timestamp(from_unixtime(col("data.start_date"))))
        .withColumn("billing_cycle_anchor_ts", to_timestamp(from_unixtime(col("data.billing_cycle_anchor"))))
        .withColumn("cancel_at_ts", to_timestamp(from_unixtime(col("data.cancel_at"))))
        .withColumn("canceled_at_ts", to_timestamp(from_unixtime(col("data.canceled_at"))))
        .withColumn("trial_start_ts", to_timestamp(from_unixtime(col("data.trial_start"))))
        .withColumn("trial_end_ts", to_timestamp(from_unixtime(col("data.trial_end"))))
        .withColumn("cancel_at_period_end", col("data.cancel_at_period_end").cast("boolean"))
        .withColumn("livemode", col("data.livemode").cast("boolean"))

        .withColumn("api_extracted_ts", to_timestamp(col("_extracted_at")))
        .withColumn("etl_run_id", lit(run_id))
        .withColumn("silver_processed_ts", current_timestamp())
        .withColumn("silver_processed_date", current_date())
    )

    return df.select(
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
        "trial_start_ts",
        "trial_end_ts",
        "cancel_at_period_end",
        "livemode",
        "api_extracted_ts",
        "_ingest_ts",
        "_ingest_date",
        "_file_name",
        "_source_format",
        "etl_run_id",
        "silver_processed_ts",
        "silver_processed_date",
    )


def _build_unknown_df(spark: SparkSession, run_id: str) -> DataFrame:
    return spark.range(1).select(
        lit(-1).cast("bigint").alias("stripe_subscriptions_sk"),
        lit("UNKNOWN").cast("string").alias("subscription_id"),
        lit(None).cast("string").alias("stripe_customer_id"),
        lit(None).cast("string").alias("subscription_status"),
        lit(None).cast("string").alias("collection_method"),
        lit(None).cast("string").alias("currency"),
        lit(None).cast("string").alias("latest_invoice_id"),
        lit(None).cast("timestamp").alias("created_ts"),
        lit(None).cast("timestamp").alias("start_date_ts"),
        lit(None).cast("timestamp").alias("billing_cycle_anchor_ts"),
        lit(None).cast("timestamp").alias("cancel_at_ts"),
        lit(None).cast("timestamp").alias("canceled_at_ts"),
        lit(None).cast("timestamp").alias("ended_at_ts"),
        lit(None).cast("timestamp").alias("trial_start_ts"),
        lit(None).cast("timestamp").alias("trial_end_ts"),
        lit(None).cast("boolean").alias("cancel_at_period_end"),
        lit(None).cast("boolean").alias("livemode"),
        lit(None).cast("timestamp").alias("api_extracted_ts"),
        lit(None).cast("string").alias("_file_name"),
        lit("system").cast("string").alias("_source"),
        lit("UNKNOWN").cast("string").alias("_landing_format"),
        lit(datetime(1900, 1, 1)).cast("timestamp").alias("silver_effective_start_ts"),
        lit(None).cast("timestamp").alias("silver_effective_end_ts"),
        current_timestamp().alias("updated_at"),
        lit(run_id).cast("string").alias("etl_run_id"),
        sha2(lit("UNKNOWN"), 256).alias("record_hash"),
        lit(True).alias("is_current"),
    )


def _build_incoming_conform_df(dedup_df: DataFrame) -> DataFrame:
    scd2_cols = [
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
        "_file_name",
        "_source",
        "_landing_format",
    ]

    return (
        dedup_df
        .withColumn("silver_effective_start_ts", col("_ingest_ts").cast("timestamp"))
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    *[coalesce(col(c).cast("string"), lit("")) for c in scd2_cols]
                ),
                256,
            ),
        )
        .select(
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
            "_file_name",
            "_source",
            "_landing_format",
            "etl_run_id",
            "silver_effective_start_ts",
            "record_hash"
        )
    )


def _merge_conform_scd2(
        spark: SparkSession,
        conform_table: str,
        incoming_df: DataFrame,
        run_id: str,
        key_columns: list[str],
) -> None:
    
    if not key_columns:
        raise ValueError("key_columns must not be empty.")
    
    missing_columns = [c for c in key_columns if c not in incoming_df.columns]

    if missing_columns:
        raise ValueError(f"key columns missing from incoming_df: {missing_columns}")
    
    conform_dt = DeltaTable.forName(spark, conform_table)

    unknown_df = _build_unknown_df(spark=spark, run_id=run_id)

    (
        conform_dt.alias("t")
        .merge(unknown_df.alias("s"), condition="t.stripe_subscriptions_sk = s.stripe_subscriptions_sk")
        .whenNotMatchedInsertAll()
        .execute()
    )

    conform_active = (
        conform_dt.toDF()
        .filter(col("is_current"))
        .select(*key_columns, "record_hash")
    )

    join_condition = " AND ".join([f"inc.{c} = con.{c}" for c in key_columns])


    joined_df = incoming_df.alias("inc").join(
        conform_active.alias("con"),
        on=join_condition,
        how="left"
    )

    changed_df = (
        joined_df
        .filter(col("con.record_hash").isNotNull() & (col("inc.record_hash") != col("con.record_hash")))
        .select("inc.*")
    )

    new_df = (
        joined_df
        .filter(col("con.record_hash").isNull())
        .select("inc.*")
    )

    update_changed_df = changed_df.withColumn("scd_action", lit("UPDATE"))

    insert_changed_df = changed_df.withColumn("scd_action", lit("INSERT"))

    insert_new_df = new_df.withColumn("scd_action", lit("INSERT"))

    staged_df = (
        update_changed_df
        .unionByName(insert_changed_df)
        .unionByName(insert_new_df)
        .withColumn("is_current", lit(True))
        .withColumn("silver_effective_end_ts", lit(None).cast("timestamp"))
        .withColumn("updated_at", current_timestamp())
    )

    merge_condition = " AND ".join([*(f"t.{c} = s.{c}" for c in key_columns), "t.is_current = true"])

    (
        conform_dt.alias("t")
        .merge(staged_df.alias("s"), condition=merge_condition)
        .whenMatchedUpdate(
            condition="t.record_hash <> s.record_hash AND s.scd_action = 'UPDATE'",
            set={
                "silver_effective_end_ts" : "s.silver_effective_start_ts",
                "updated_at": "s.updated_at",
                "is_current": "false",
                "etl_run_id": "s.etl_run_id"
            }
        )
        .whenNotMatchedInsert(
            condition="s.scd_action = 'INSERT'",
            values={
                "subscription_id": "s.subscription_id",
                "stripe_customer_id": "s.stripe_customer_id",
                "subscription_status": "s.subscription_status",
                "collection_method": "s.collection_method",
                "currency": "s.currency",
                "latest_invoice_id": "s.latest_invoice_id",
                "created_ts": "s.created_ts",
                "start_date_ts": "s.start_date_ts",
                "billing_cycle_anchor_ts" : "s.billing_cycle_anchor_ts",
                "cancel_at_ts": "s.cancel_at_ts",
                "canceled_at_ts": "s.canceled_at_ts",
                "ended_at_ts": "s.ended_at_ts",
                "trial_start_ts": "s.trial_start_ts",
                "trial_end_ts": "s.trial_end_ts",
                "cancel_at_period_end": "s.cancel_at_period_end",
                "livemode": "s.livemode",
                "_file_name": "s._file_name",
                "_source": "s._source",
                "_landing_format": "s._landing_format",
                "etl_run_id": "s.etl_run_id",
                "silver_effective_start_ts": "s.silver_effective_start_ts",
                "silver_effective_end_ts": "s.silver_effective_end_ts",
                "record_hash": "s.record_hash",
                "is_current": "s.is_current"
            }
        )
        .execute()
    )

    



def run_silver_stripe_subscriptions(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "silver_stripe_subscriptions"
    dataset = "stripe_subscriptions"
    run_id = uuid.uuid4().hex

    cfg = _build_config(spark=spark, env=env)

    rows_in = 0
    rows_out = 0
    rows_quarantined = 0
    last_wm = None
    new_wm = None
    dq_result = "OK"

    business_key = ["subscription_id"]
    order_columns = ["_ingest_ts", "silver_processed_ts"]

    insert_run_log_start(
        spark=spark,
        run_logs_table=cfg["run_logs_table"],
        pipeline_name=pipeline_name,
        dataset=dataset,
        target_table=cfg["conform_table"],
        run_id=run_id,
    )

    try:

        logger.info("Silver stripe_subscriptions start | run_id=%s", run_id)
        
        # Read bronze and validate data source
        bronze_df = spark.read.format("delta").load(cfg["bronze_path"])

        required_columns = _get_required_columns()

        missing_columns = [c for c in required_columns if c not in bronze_df.columns]

        if missing_columns:
            raise ValueError(f"missing required columns: {missing_columns}")

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
                last_watermark_ts=last_wm
            )
            return
        
        stage_df = _build_stage_stripe_subscriptions(
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

        # conform creation

        incoming_df = _build_incoming_conform_df(dedup_df=dedup_df)

        _merge_conform_scd2(
            spark=spark,
            conform_table=cfg["conform_table"],
            incoming_df=incoming_df,
            run_id=run_id,
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

        logger.info("Silver stripe_subscriptions | rows_in = %d | rows_out = %d | rows_quarantined =%d",
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

        logger.exception("Silver strip_customers FAILED")
        raise