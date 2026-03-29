import uuid
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (col, lit, current_timestamp, current_date, when, concat_ws, row_number)
from pyspark.sql.window import Window

from services.envs import EnvConfig
from services.watermark import read_incremental_by_watermark, upsert_watermark
from services.audit import (
    insert_run_log_start,
    update_run_log_no_new_data,
    update_run_log_success,
    update_run_log_failure
)
from services.delta_table import write_overwrite_table, write_append_table
from services.dq import evaluate_null_rules, build_dq_results_df, build_dq_failure_message
from services.snapshot import merge_current_snapshot

logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table": f"{env.catalog}.{env.project}_ops.pipeline_state",
        "stage_table": f"{env.catalog}.{env.project}_silver.stage_usage",
        "quarantine_table": f"{env.catalog}.{env.project}_silver.quarantine_usage",
        "conform_table": f"{env.catalog}.{env.project}_silver.conform_usage",
        "dq_table": f"{env.catalog}.{env.project}_silver.dq_usage",

        "bronze_path": f"{env.raw_base_path}/{env.project}/fact_usage_daily",
        "stage_path": f"{env.curated_base_path}/{env.project}/fact_usage_daily/stage_fact_usage_daily",
        "quarantine_path": f"{env.curated_base_path}/{env.project}/fact_usage_daily/quarantine_fact_usage_daily",
        "conform_path": f"{env.curated_base_path}/{env.project}/fact_usage_daily/conform_fact_usage_daily",
        "dq_path": f"{env.curated_base_path}/{env.project}/fact_usage_daily/data_quality",
    }

def _get_required_cols() -> list:
    return [
        "dt",
        "account_id",
        "feature",
        "active_users",
        "units"
    ]

def _build_stage_df(incr_df: DataFrame, run_id: str) -> DataFrame:
    return (
        incr_df
        .withColumn("dt", col("dt").cast("date"))
        .withColumn("account_id", col("account_id").cast("string"))
        .withColumn("feature", col("feature").cast("string"))
        .withColumn("active_users", col("active_users").cast("int"))
        .withColumn("units", col("units").cast("int"))
        .withColumn("etl_run_id", lit(run_id))
        .withColumn("silver_processed_ts", current_timestamp())
        .withColumn("silver_processed_date", current_date())
    )

def _split_quarantine(df: DataFrame):

    bad_conditions = (
        col("account_id").isNull()
        | col("feature").isNull()
        | col("active_users").isNull() | (col("active_users") < 0)
        | col("units").isNull() | (col("units") < 0)
    )

    bad_records = (df
                   .filter(when(bad_conditions))
                   .withColumn(
                       "_quarantine_reason",
                       concat_ws(
                           ";",
                           when(col("account_id").isNull(), lit("account_id is NULL")),
                           when(col("feature").isNull(), lit("feature is NULL")),
                           when(col("active_users").isNull(), lit("active_users is NULL")),
                           when(col("active_users") < 0, lit("active_users < 0")),
                           when(col("units").isNull(), lit("units is NULL")),
                           when(col("units") < 0, lit("units < 0")),
                       )
                    )
                    .withColumn("_quarantine_ts", current_timestamp())
                )
    
    good_records = (df.filter(~bad_conditions))

    return bad_records, good_records



def _deduplication(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("account_id").orderBy(col("_ingest_ts").desc_nulls_last(), col("silver_processed_ts").desc_nulls_last())

    return (
        df
        .withColumn("_rn", row_number().over(window_spec))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )



def run_silver_fact_usage_daily(spark: SparkSession, env: EnvConfig) -> None:

    pipeline_name = "silver_fact_usage_daily"
    dataset = "fact_usage_daily"
    run_id = uuid.uuid4().hex
    
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

        logger.info("Silver fact_usage_daily | run_id =%s", run_id)

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

        if incr_df.take(1) == []:
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
        
        stage_df = _build_stage_df(
            incr_df=incr_df,
            run_id=run_id,
        )

        write_overwrite_table(
            spark=spark,
            df=stage_df,
            table_name=cfg["stage_table"],
            table_path=cfg["stage_path"],
        )
        stage_df = spark.read.table(cfg["stage_path"])

        bad_records, good_records = _split_quarantine(stage_df)

        write_append_table(
            spark=spark,
            df=bad_records,
            table_name=cfg["quarantine_table"],
            table_path=cfg["quarantine_path"],
        )

        dedup_df = _deduplication(good_records)

        rows_in = stage_df.count()
        rows_out = dedup_df.count()
        rows_quarantined = bad_records.count()




        dq_source_df = stage_df
        dq_source_table = spark.read.table(cfg["dq_table"])

        dq_rules = env.datasets[dataset]["data_quality"]["rules"]
        dq_metrics = evaluate_null_rules(df=dq_source_df, rules=dq_rules)
        dq_result = dq_metrics["overall_result"]


        dq_result_df = build_dq_results_df(
            spark=spark,
            table_name=dq_source_table,
            run_id=run_id,
            dq_scope="batch",
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
        
        merge_current_snapshot(
            spark=spark,
            current_path=cfg["current_path"],
            current_table=cfg["current_table"],
            df=dedup_df,
            pk="account_id",
        )
        current_df = spark.read.table(cfg["current_table"])


        
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
            "Silver fact_usage_daily SUCCESS | rows_in=%d | rows_out=%d | rows_quarantined=%d",
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
        logger.info("Silver fact_usage_daily FAILED")
        raise