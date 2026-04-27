from __future__ import annotations

import logging
import uuid

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
)
from delta.tables import DeltaTable
from services.envs import EnvConfig
from services.audit import (
    insert_run_log_start,
    update_run_log_no_new_data,
    update_run_log_success,
    update_run_log_failure
)


logger = logging.getLogger(__name__)

def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "silver_current_table": f"{env.catalog}.{env.project}_silver.s_current_erp_usage_daily",

        "gold_dim_account_table": f"{env.catalog}.{env.project}_gold.g_dim_account",
        "gold_dim_plan_table": f"{env.catalog}.{env.project}_gold.g_dim_plan",

        "gold_fact_table": f"{env.catalog}.{env.project}_gold.g_fact_usage_daily",
        "gold_fact_path": f"{env.gold_base_path}/{env.catalog}/{env.project}/erp_usage_daily/g_fact_usage_daily",

    }

def _get_required_columns() -> list[str]:
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
        "_landing_format",
        "etl_run_id",
    ]

def _build_stage_gold_fact_usage_daily(
        silver_df: DataFrame,
        dim_account_df: DataFrame,
        dim_plan_df: DataFrame,
) -> DataFrame:
    
    stage_df = (
        silver_df
        .withColumn("usage_id", lower(trim(col("usage_id"))).cast("string"))
        .withColumn("event_ts", col("event_ts").cast("timestamp"))
        .withColumn("usage_date", col("usage_date").cast("date"))
        .withColumn("account_id", trim(col("account_id")).cast("string"))
        .withColumn("feature_code", lower(trim(col("feature_code"))).cast("string"))
        .withColumn("active_users", col("active_users").cast("int"))
        .withColumn("units_raw", col("units_raw").cast("int"))
        .withColumn("source_system", upper(trim(col("source_system"))).cast("string"))
        .withColumn("batch_id", trim(col("batch_id")).cast("string"))
    )

    account_current_df = (
        dim_account_df
        .filter(col("is_current") == True)
        .select(
            col("account_master_snapshot_sk"),
            col("account_id"),
            col("plan_code")
        )
    )

    plan_current_df = (
        dim_plan_df
        .filter(col("is_current") == True)
        .select(
            col("plan_catalog_sk"),
            col("plan_code")
        )
    )

    joined_df = (
        stage_df.alias("f")
        .join(
            account_current_df.alias("a"),
            col("f.account_id") == col("a.account_id"),
            "left",
        )
        .join(
            plan_current_df.alias("p"),
            col("a.plan_code") == col("p.plan_code"),
            "left"
        )
    )

    return (
        joined_df
        .withColumn("account_master_snapshot_sk", coalesce(col("a.account_master_snapshot_sk"), lit(-1)))
        .withColumn("plan_catalog_sk", coalesce(col("p.plan_catalog_sk"), lit(-1)))
        .withColumn("gold_loaded_ts", current_timestamp())
        .withColumn("gold_loaded_date", current_date())
        .select(
            col("f.usage_id").alias("usage_id"),
            col("account_master_snapshot_sk"),
            col("plan_catalog_sk"),
            col("f.event_ts").alias("event_ts"),
            col("f.usage_date").alias("usage_date"),
            col("f.account_id").alias("account_id"),
            col("f.feature_code").alias("feature_code"),
            col("f.active_users").alias("active_users"),
            col("f.units_raw").alias("units_raw"),
            col("f.source_system").alias("source_system"),
            col("f.batch_id").alias("batch_id"),
            col("f._file_name").alias("_file_name"),
            col("f._source").alias("_source"),
            col("f._landing_format").alias("_landing_format"),
            col("f.etl_run_id").alias("etl_run_id"),
            col("gold_loaded_ts"),
            col("gold_loaded_date"),
        )
    )


def run_gold_fact_usage_daily(spark: SparkSession, env: EnvConfig) -> None:
    
    pipeline_name = "gold_fact_usage_daily"
    dataset = "fact_usage_daily"
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

        logger.info("Gold fact_usage_daily start | run_id=%s", run_id)

        silver_df = spark.table(cfg["silver_current_table"])
        dim_account_df = spark.table(cfg["gold_dim_account_table"])
        dim_plan_df = spark.table(cfg["gold_dim_plan_table"])

        required_columns = _get_required_columns()

        missing_columns = [c for c in required_columns if c not in silver_df.columns]
        if missing_columns:
            raise ValueError(f"Silver current missing required cols: {missing_columns}")
        
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
        
        gold_df = _build_stage_gold_fact_usage_daily(
            silver_df=silver_df,
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
                "t.usage_id = s.usage_id",
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
            rows_quarantined=0,
            dq_result="ERROR",
            last_watermark_ts=None,
        )
        logger.exception("Gold fact_usage_daily FAILED")
        raise