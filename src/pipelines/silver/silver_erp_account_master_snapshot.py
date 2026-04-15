import uuid
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    trim,
    col,
    lower,
    upper,
    when,
    coalesce,
    to_date,
    lit,
    current_timestamp,
    current_date,
    sha2,
    concat_ws
    
)
from datetime import datetime
from delta.tables import DeltaTable

from services.envs import EnvConfig
from services.audit import (
    insert_run_log_start,
    update_run_log_no_new_data,
    update_run_log_success,
    update_run_log_failure
)
from services.watermark import read_incremental_by_watermark, upsert_watermark
from services.dq import evaluate_dq_rules, build_dq_results_df, build_dq_failure_message
from services.delta_table import write_append_table
from services.dq import quarantine_by_business_key
from services.transformations import deduplicate_by_business_key
from services.snapshot import merge_current_snapshot

logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table": f"{env.catalog}.{env.project}_ops.pipeline_state",
        "dq_table": f"{env.catalog}.{env.project}_silver.s_dq_erp_account_master_snapshot",
        "quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_erp_account_master_snapshot",
        "current_table": f"{env.catalog}.{env.project}_silver.s_current_erp_account_master_snapshot",
        "conform_table": f"{env.catalog}.{env.project}_silver.s_conform_erp_account_master_snapshot",

        "bronze_path": f"{env.raw_base_path}/{env.catalog}/b_erp_account_master_snapshot",
        "dq_path": f"{env.curated_base_path}/{env.catalog}/erp_account_master_snapshot/s_dq_erp_account_master_snapshot",
        "quarantine_path": f"{env.curated_base_path}/{env.catalog}/erp_account_master_snapshot/s_quarantine_erp_account_master_snapshot",
        "current_path": f"{env.curated_base_path}/{env.catalog}/erp_account_master_snapshot/s_current_erp_account_master_snapshot",
        "conform_path": f"{env.curated_base_path}/{env.catalog}/erp_account_master_snapshot/s_conform_erp_account_master_snapshot",
    }

def _get_required_columns() -> list[str]:
    return [
        "account_id",
        "customer_name",
        "email",
        "stripe_customer_id",
        "plan_code",
        "segment",
        "country_code",
        "region",
        "account_created_at_raw",
        "status",
        "churned_at_raw",
        "source_system",
        "snapshot_dt",
        "_ingest_ts",
        "_ingest_date",
        "_file_name",
        "_source",
        "_landing_format",
    ]

def _build_stage_erp_account_master_snapshot(incr_df: DataFrame, run_id: str) -> DataFrame:
    df = (
        incr_df
        .withColumn("account_id", trim(col("account_id")).cast("string"))
        .withColumn("customer_name", trim(col("customer_name")).cast("string"))
        .withColumn("email", lower(trim(col("email"))).cast("string"))
        .withColumn("stripe_customer_id", trim(col("stripe_customer_id")).cast("string"))
        .withColumn("plan_code", lower(trim(col("plan_code"))).cast("string"))
        .withColumn("segment", upper(trim(col("segment"))).cast("string"))
        .withColumn(
            "country_code",
            when(trim(col("country_code")).isNull(), None)
            .when(upper(trim(col("country_code"))) == "UK", "GB")
            .when(upper(trim(col("country_code"))) == "U.S.", "US")
            .when(upper(trim(col("country_code"))) == "USA", "US")
            .when(upper(trim(col("country_code"))) == "DEU", "DE")
            .when(upper(trim(col("country_code"))) == "ROU", "RO")
            .otherwise(upper(trim(col("country_code"))))
        )
        .withColumn(
            "region",
            when(trim(col("region")).isNull() & col("country_code").isin(
                "GB", "DE", "RO", "FR", "IT", "ES", "NL", "SE", "DK", "NO", "PL"
            ), lit("EMEA"))
            .when(trim(col("region")).isNull() & col("country_code").isin("AU", "JP", "SG", "IN"), lit("APAC"))
            .when(trim(col("region")).isNull() & col("country_code").isin("US", "CA"), lit("AMER"))
            .otherwise(upper(trim(col("region"))))
        )
        .withColumn(
            "region",
            when(col("region").isin("EMEA", "APAC", "AMER", "LATAM"), col("region"))
            .otherwise(lit(None))
            )

        .withColumn(
            "account_created_at",
            coalesce(
                to_date(trim(col("account_created_at")), "yyyy-MM-dd"),
                to_date(trim(col("account_created_at")), "dd/MM/yyyy")
            )
        )
        .withColumn(
            "churned_at",
            coalesce(
                to_date(trim(col("churned_at")), "yyyy-MM-dd"),
                to_date(trim(col("churned_at")), "dd/MM/yyyy")
            )
        )
        .withColumn(
            "snapshot_at",
            coalesce(
                to_date(trim(col("snapshot_at")), "yyyy-MM-dd"),
                to_date(trim(col("snapshot_at")), "dd/MM/yyyy")
            )
        )
        .withColumn(
            "status",
            when(trim(col("status")).isNull(), None)
            .otherwise(lower(trim(col("status"))))
        )
        .withColumn("etl_run_id", lit(run_id))
        .withColumn("silver_processed_ts", current_timestamp())
        .withColumn("silver_processed_date", current_date())
    )

    return df.select(
        "account_id",
        "customer_name",
        "email",
        "stripe_customer_id",
        "plan_code",
        "segment",
        "country_code",
        "region",
        "account_created_at_raw",
        "status",
        "churned_at_raw",
        "source_system",
        "snapshot_dt",
        "_ingest_ts",
        "_ingest_date",
        "_file_name",
        "_source",
        "_landing_format",
        "etl_run_id",
        "silver_processed_ts",
        "silver_processed_date"
    )

def _build_unknown_conform_df(spark: SparkSession, run_id: str) -> DataFrame:
    return spark.range(1).select(
        lit(-1).cast("bigint").alias("account_master_snapshot_sk"),
        lit("UNKNOWN").cast("string").alias("account_id"),
        lit(None).cast("string").alias("customer_name"),
        lit(None).cast("string").alias("email"),
        lit(None).cast("string").alias("stripe_customer_id"),
        lit(None).cast("string").alias("plan_code"),
        lit(None).cast("string").alias("segment"),
        lit(None).cast("string").alias("country_code"),
        lit(None).cast("string").alias("region"),
        lit(datetime(1900, 1, 1)).cast("date").alias("account_created_at_raw"),
        lit(None).cast("string").alias("status"),
        lit(None).cast("date").alias("churned_at_raw"),
        lit(None).cast("string").alias("source_system"),
        lit(None).cast("date").alias("snapshot_dt"),
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
        "account_id",
        "customer_name",
        "email",
        "stripe_customer_id",
        "plan_code",
        "segment",
        "country_code",
        "region",
        "account_created_at_raw",
        "status",
        "churned_at_raw",
        "source_system",
        "snapshot_dt",
        "_file_name",
        "_source",
        "_landing_format",
    ]

    return (
        dedup_df
        .withColumn("silver_effective_start_ts", col("_ingest_ts").cast("timestamp"))
        .withColumn("record_hash", sha2(concat_ws(";", *[coalesce(col(c).cast("string"), lit("")) for c in scd2_cols]), 256))
        .select(
            "account_id",
            "customer_name",
            "email",
            "stripe_customer_id",
            "plan_code",
            "segment",
            "country_code",
            "region",
            "account_created_at_raw",
            "status",
            "churned_at_raw",
            "source_system",
            "snapshot_dt",
            "_file_name",
            "_source",
            "_landing_format",
            "etl_run_id",
            "silver_effective_start_ts",
            "record_hash",
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
        raise ValueError(f"Key columns missing from incoming_df: {missing_columns}")
    
    conform_dt = DeltaTable.forName(spark, conform_table)
    
    unknown_df = _build_unknown_conform_df(spark=spark, run_id=run_id)

    (
        conform_dt.alias("t")
        .merge(unknown_df.alias("s"), "t.account_master_snapshot_sk = s.account_master_snapshot_sk")
        .whenNotMatchedInsertAll()
        .execute()
    )

    conform_active = (
        conform_dt.toDF()
        .filter(col("is_current"))
        .select(*key_columns, "record_hash")
    )

    join_condition = " AND ".join([f"t.{c} = s.{c}" for c in key_columns])

    joined_df = incoming_df.alias("inc").join(
        conform_active.alias("con"),
        on=join_condition,
        how="left"
    )

    changed_df = (
        joined_df
        .filter(col("con.record_hash").isNotNull() & col("con.record_hash") != col("inc.record_hash"))
        .select("inc.*")
    )

    new_df = (
        joined_df
        .filter(col("con.record_hash").isNull())
        .select("inc.*")
    )

    update_changed_df = (
        changed_df
        .select("*")
        .withColumn("scd_action", lit("UPDATE"))
    )

    insert_changed_df = (
        changed_df
        .select("*")
        .withColumn("scd_action", lit("INSERT"))
    )

    insert_new_df = (
        new_df
        .select("*")
        .withColumn("scd_action", lit("INSERT"))
    )

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
        .merge(staged_df.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition="t.record_hash <> s.record_hash AND s.scd_action = 'UPDATE'",
            set={
                "silver_effective_end_ts": "s.silver_effective_start_ts",
                "updated_at": "current_timestamp()",
                "is_current": "false",
                "etl_run_id": "s.etl_run_id",
            }
        )
        .whenNotMatchedInsert(
            condition="s.scd_action = 'INSERT'",
            values={
                "account_id" : "s.accound_id",
                "customer_name": "s.customer_name",
                "email": "s.email",
                "stripe_customer_id": "s.stripe_customer_id",
                "plan_code": "s.plan_code",
                "segment": "s.segment",
                "country_code": "s.country_code",
                "region": "s.region",
                "account_created_at_raw": "s.account_created_at_raw",
                "status": "s.status",
                "churned_at_raw": "s.churned_at_raw",
                "source_system": "s.source_system",
                "snapshot_dt": "s.snapshot_dt",
                "_file_name": "s._file_name",
                "_source": "s._source",
                "_landing_format": "s._landing_format",
                "silver_effective_start_ts": "s.silver_effective_start_ts",
                "silver_effective_end_ts": "s.silver_effective_end_ts",
                "updated_at": "s.updated_at",
                "etl_run_id": "s.etl_run_id",
                "record_hash": "s.record_hash",
                "is_current": "s.is_current",
            }
        )
        .execute()
    )




def run_silver_erp_account_master(spark: SparkSession, env: EnvConfig) -> None:

    pipeline_name = "silver_erp_account_master_snapshot"
    dataset = "erp_account_master_snapshot"
    run_id = uuid.uuid4().hex
    business_key = ["account_id"]

    cfg = _build_config(env)

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
        target_table=cfg["conform_table"],
        run_id=run_id
    )   

    try:
        
        logger.info("Silver_erp_account_master_snapshot | run_id=%s.", run_id)

        # Read bronze and validate data source
        bronze_df = spark.read.format("delta").load(cfg["bronze_path"])

        required_columns = _get_required_columns()

        missing_columns = [c for c in required_columns if c not in bronze_df.columns]

        if missing_columns:
            raise ValueError(f"missing required columns: {missing_columns}")
        
        # incremental and handle wms
        incr_df, last_wm, new_wm = read_incremental_by_watermark(
            spark=spark,
            source_df=bronze_df,
            state_table=cfg["state_table"],
            pipeline_name=pipeline_name,
            dataset=dataset,
            watermark_col="_ingest_ts"
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

        # staging

        stage_df = _build_stage_erp_account_master_snapshot(
            incr_df=incr_df,
            run_id=run_id,
        )

        # dq
        dq_source = "stage_erp_account_master_snapshot"


        dq_rules = env.datasets["erp_account_master_snapshot"]["data_quality"]["rules"]
        dq_metrics = evaluate_dq_rules(df=stage_df, rules=dq_rules)
        dq_result = dq_metrics["overall_result"]

        dq_df = build_dq_results_df(
            spark=spark,
            dq_source=dq_source,
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

        # conform creation

        incoming_df = _build_incoming_conform_df(dedup_df=dedup_df)
        _merge_conform_scd2(
            spark=spark,
            conform_table=cfg["conform_table"],
            incoming_df=incoming_df,
            run_id=run_id,
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

        logger.info("Silver erp_account_master_snapshot SUCCESS | row_in=%d | rows_out=%d | rows_quarantined=%d",
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
        logger.exception("Silver_erp_account_master_snapshot FAILED")
        raise