import uuid
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (col, lit, lower, upper, trim, coalesce, to_date, current_timestamp, current_date)
from services.envs import EnvConfig
from services.watermark import read_incremental_by_watermark, upsert_watermark
from services.audit import (
    insert_run_log_start,
    update_run_log_no_new_data,
    update_run_log_success,
    update_run_log_failure
)
from services.delta_table import write_append_table
from services.dq import evaluate_dq_rules, build_dq_results_df, build_dq_failure_message, quarantine_by_business_key
from services.snapshot import merge_current_snapshot
from services.transformations import deduplicate_by_business_key

logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table": f"{env.catalog}.{env.project}_ops.pipeline_state",
        "dq_table": f"{env.catalog}.{env.project}_silver.s_dq_erp_usage_daily",
        "quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_erp_usage_daily",
        "current_table": f"{env.catalog}.{env.project}_silver.s_current_erp_usage_daily",

        "bronze_path": f"{env.raw_base_path}/{env.project}/erp_usage_daily",
        "dq_path": f"{env.curated_base_path}/{env.project}/erp_usage_daily/s_dq_erp_usage_daily",
        "quarantine_path": f"{env.curated_base_path}/{env.project}/erp_usage_daily/s_quarantine_erp_usage_daily",
        "current_path": f"{env.curated_base_path}/{env.project}/erp_usage_daily/s_current_erp_usage_daily",
        
    }

def _get_required_cols() -> list:
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
        "_ingest_ts",
        "_ingest_date",
        "_file_name",
        "_source",
        "_landing_format"
    ]

def _build_stage_silver_erp_usage_daily(incr_df: DataFrame, run_id: str) -> DataFrame:
    df = (
        incr_df
        .withColumn("usage_id", lower(trim(col("usage_id"))).cast("string"))
        .withColumn("event_ts", col("event_ts").cast("timestamp"))
        .withColumn("usage_date", coalesce(
            to_date(trim(col("usage_date"), "yyyy-MM-dd")),
            to_date(trim(col("usage_date"), "dd-MM-yyyy")))
            )
        .withColumn("account_id", trim(col("account_id")).cast("string"))
        .withColumn("feature_code", lower(trim(col("feature_code"))).cast("string"))
        .withColumn("active_users", col("active_users").cast("int"))
        .withColumn("units_raw", col("units_raw").cast("int"))
        .withColumn("source_system", upper(trim(col("units_raw"))).cast("string"))
        .withColumn("batch_id", trim(col("batch_id")).cast("string"))
        .withColumn("etl_run_id", lit(run_id))
        .withColumn("silver_processed_ts", current_timestamp())
        .withColumn("silver_processed_date", current_date())
    )

    return df.select(
        "usage_id",
        "event_ts",
        "usage_date",
        "account_id",
        "feature_code",
        "active_users",
        "units_raw",
        "source_system",
        "batch_id",
        "_ingest_ts",
        "_ingest_date",
        "_file_name",
        "_source",
        "_landing_format",
        "etl_run_id",
        "silver_processed_ts",
        "silver_processed_date"
    )


def run_silver_erp_usage_daily(spark: SparkSession, env: EnvConfig) -> None:

    pipeline_name = "silver_erp_usage_daily"
    dataset = "erp_usage_daily"
    run_id = uuid.uuid4().hex
    business_key = ["usage_id"]
    
    cfg = _build_config(env=env)

    rows_in = 0
    rows_out = 0
    rows_quarantined = 0
    last_wm = None
    new_wm = None
    dq_result = "OK"

    insert_run_log_start(
        spark=spark,
        run_logs_table=cfg["run_logs_table"],
        pipeline_name=pipeline_name,
        dataset=dataset,
        target_table=cfg["conform_table"]
    )

    try:

        logger.info("Silver erp_usage_daily | run_id =%s", run_id)

        bronze_df = spark.read.format("delta").load(cfg["bronze_path"])

        required_columns = _get_required_cols()

        missing_columns = [c for c in required_columns if c not in bronze_df.columns]

        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

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
            logger.info("No new data. Exiting")
            return
        

        # staging
        stage_df = _build_stage_silver_erp_usage_daily(
            incr_df=incr_df,
            run_id=run_id,
        )


        # dq
        dq_source_table = "stage_silver_erp_usage_daily"

        dq_rules = env.datasets["erp_usage_daily"]["data_quality"]["rules"]
        dq_metrics = evaluate_dq_rules(df=stage_df, rules=dq_rules)
        dq_result = dq_metrics["overall_result"]

        dq_df = build_dq_results_df(
            spark=spark,
            dq_source=dq_source_table,
            run_id=run_id,
            metrics=dq_metrics
        )

        write_append_table(
            spark=spark,
            df=dq_df,
            table_name=cfg["dq_table"],
            table_path=cfg["dq_path"]
        )

        if dq_result == "FAIL":
            raise ValueError(build_dq_failure_message(dq_metrics))
        

        # quarantination
        bad_records, good_records = quarantine_by_business_key(
            stage_df=stage_df,
            key_columns=business_key
        )

        write_append_table(
            spark=spark,
            df=bad_records,
            table_name=cfg["quarantine_table"],
            table_path=cfg["quarantine_path"]
        )

        # deduplication
        order_columns = ["_ingest_ts", "silver_processed_ts"]
        dedup_df = deduplicate_by_business_key(
            df=good_records,
            key_columns=business_key,
            order_columns=order_columns
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
            last_watermark_ts=new_wm,
        )

        logger.info(
            "Silver erp_usage_daily SUCCESS | rows_in=%d | rows_out=%d | rows_quarantined=%d",
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
            error_msg=str(e),
            rows_in=rows_in,
            rows_out=rows_out,
            rows_quarantined=rows_quarantined,
            dq_result=dq_result,
            last_watermark_ts=last_wm,
        )
        logger.info("Silver erp_usage_daily FAILED")
        raise