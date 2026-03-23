import uuid
import logging
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    col,
    lit,
    row_number,
    count,
    sum,
    when,
    concat_ws,
    sha2,
    coalesce,
    max,
)
from pyspark.sql.window import Window

from services.envs import EnvConfig
from services.watermark import get_last_watermark, upsert_watermark

logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        # tables
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table": f"{env.catalog}.{env.project}_ops.pipeline_state",
        "stage_table": f"{env.catalog}.{env.project}_silver.stage_plan",
        "current_table": f"{env.catalog}.{env.project}_silver.current_plan",
        "quarantine_table": f"{env.catalog}.{env.project}_silver.quarantine_plan",
        "conform_table": f"{env.catalog}.{env.project}_silver.conform_plan",
        "dq_table": f"{env.catalog}.{env.project}_silver.dq_plan",
        # paths
        "bronze_path": f"{env.raw_base_path}/{env.project}/dim_plan",
        "stage_path": f"{env.curated_base_path}/{env.project}/dim_plan/stage_dim_plan",
        "current_path": f"{env.curated_base_path}/{env.project}/dim_plan/current_dim_plan",
        "quarantine_path": f"{env.curated_base_path}/{env.project}/dim_plan/quarantine_dim_plan",
        "conform_path": f"{env.curated_base_path}/{env.project}/dim_plan/conform_dim_plan",
        "dq_path": f"{env.curated_base_path}/{env.project}/dim_plan/data_quality",
    }


def run_silver_dim_plan(spark: SparkSession, env: EnvConfig, dq_scope: str = "batch") -> None:
    dq_scope = (dq_scope or "batch").strip().lower()
    if dq_scope not in ("batch", "current"):
        raise ValueError(f"dq_scope must be 'batch' or 'current'. Got: {dq_scope}")

    pipeline_name = "silver_dim_plan"
    dataset = "dim_plan"
    run_id = uuid.uuid4().hex
    pk = "plan_id"

    cfg = _build_config(env)

    spark.sql(f"""
        INSERT INTO {cfg["run_logs_table"]} (
            pipeline_name, dataset, target_table, run_id, started_at, status
        )
        VALUES (
            '{pipeline_name}',
            '{dataset}',
            '{cfg["conform_table"]}',
            '{run_id}',
            current_timestamp(),
            'RUNNING'
        )
    """)

    rows_in = 0
    rows_out = 0
    rows_quarantined = 0
    last_wm = None
    new_wm = None
    dq_result = "OK"

    try:
        logger.info("Silver dim_plan start | run_id=%s | dq_scope=%s", run_id, dq_scope)

        bronze_df = spark.read.format("delta").load(cfg["bronze_path"])

        required_cols = [
            "plan_id",
            "plan_name",
            "monthly_price_usd",
            "seats_included",
            "max_units_per_month",
            "_ingest_ts",
            "_ingest_date",
            "_file_name",
            "_source",
            "_landing_format",
        ]

        missing = [c for c in required_cols if c not in bronze_df.columns]
        if missing:
            raise ValueError(f"Bronze missing required cols: {missing}")

        last_wm = get_last_watermark(spark, cfg["state_table"], pipeline_name, dataset)
        incr_df = bronze_df.filter(col("_ingest_ts") > lit(last_wm))

        if incr_df.take(1) == []:
            spark.sql(f"""
                UPDATE {cfg["run_logs_table"]}
                SET finished_at = current_timestamp(),
                    status = 'SUCCESS',
                    dq_result = 'SKIPPED_NO_NEW_DATA',
                    rows_in = 0,
                    rows_quarantined = 0,
                    rows_out = 0,
                    last_watermark_ts = TIMESTAMP '{last_wm.isoformat(sep=" ")}'
                WHERE pipeline_name = '{pipeline_name}'
                  AND dataset = '{dataset}'
                  AND run_id = '{run_id}'
            """)
            logger.info("No new data. Exiting.")
            return

        new_wm = incr_df.agg(max(col("_ingest_ts")).alias("mx")).collect()[0]["mx"]

        # stage
        stage_df = (
            incr_df
            .withColumn(pk, col(pk).cast("string"))
            .withColumn("plan_name", col("plan_name").cast("string"))
            .withColumn("monthly_price_usd", col("monthly_price_usd").cast("bigint"))
            .withColumn("seats_included", col("seats_included").cast("bigint"))
            .withColumn("max_units_per_month", col("max_units_per_month").cast("bigint"))
            .withColumn("etl_run_id", lit(run_id))
            .withColumn("silver_processed_ts", current_timestamp())
            .withColumn("silver_processed_date", current_date())
        )

        (
            stage_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(cfg["stage_path"])
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {cfg["stage_table"]}
            USING DELTA
            LOCATION '{cfg["stage_path"]}'
        """)

        stage_df = spark.read.table(cfg["stage_table"])

        # quarantine
        bad_records = (
            stage_df
            .filter(col(pk).isNull())
            .withColumn("_quarantine_reason", lit(f"{pk} is NULL"))
            .withColumn("_quarantine_ts", current_timestamp())
        )

        good_records = stage_df.filter(col(pk).isNotNull())

        (
            bad_records.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(cfg["quarantine_path"])
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {cfg["quarantine_table"]}
            USING DELTA
            LOCATION '{cfg["quarantine_path"]}'
        """)

        # dedup
        w = Window.partitionBy(pk).orderBy(
            col("_ingest_ts").desc_nulls_last(),
            col("silver_processed_ts").desc_nulls_last(),
        )

        dedup = (
            good_records
            .withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        rows_in = stage_df.count()
        rows_quarantined = bad_records.count()
        rows_out = dedup.count()

        # current snapshot (SCD1)
        if not DeltaTable.isDeltaTable(spark, cfg["current_path"]):
            (
                dedup.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(cfg["current_path"])
            )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {cfg["current_table"]}
            USING DELTA
            LOCATION '{cfg["current_path"]}'
        """)

        current_dt = DeltaTable.forName(spark, cfg["current_table"])
        (
            current_dt.alias("t")
            .merge(dedup.alias("s"), f"t.{pk} = s.{pk}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        current_df = spark.read.table(cfg["current_table"])

        # DQ gate
        dq_source = stage_df if dq_scope == "batch" else current_df
        dq_source_table = cfg["stage_table"] if dq_scope == "batch" else cfg["current_table"]

        dq_agg = dq_source.agg(
            count(lit(1)).alias("total_rows"),
            sum(when(col(pk).isNull(), 1).otherwise(0)).alias("pk_null"),
            sum(when(col("monthly_price_usd").isNull(), 1).otherwise(0)).alias("monthly_price_usd_null"),
            sum(when(col("seats_included").isNull(), 1).otherwise(0)).alias("seats_included_null"),
        ).collect()[0]

        total_rows = int(dq_agg["total_rows"])
        pk_null = int(dq_agg["pk_null"])
        monthly_price_usd_null = int(dq_agg["monthly_price_usd_null"])
        seats_included_null = int(dq_agg["seats_included_null"])

        pk_null_pct = (pk_null / total_rows) if total_rows else 0.0
        monthly_price_usd_null_pct = (monthly_price_usd_null / total_rows) if total_rows else 0.0
        seats_included_null_pct = (seats_included_null / total_rows) if total_rows else 0.0

        dq_rules = env.datasets["dim_plan"]["data_quality"]["rules"]
        dq_max_pk_null = float(dq_rules["plan_id"]["max_null"])
        dq_max_monthly_price_usd_null = float(dq_rules["monthly_price_usd"]["max_null"])
        dq_max_seats_included_null = float(dq_rules["seats_included"]["max_null"])

        dq_result = "OK"
        if (
            (pk_null_pct > dq_max_pk_null)
            or (monthly_price_usd_null_pct > dq_max_monthly_price_usd_null)
            or (seats_included_null_pct > dq_max_seats_included_null)
        ):
            dq_result = "FAIL"

        dq_df = spark.createDataFrame(
            [(
                dq_source_table,
                run_id,
                datetime.utcnow(),
                dq_scope,
                total_rows,
                pk_null,
                monthly_price_usd_null,
                seats_included_null,
                float(pk_null_pct),
                float(monthly_price_usd_null_pct),
                float(seats_included_null_pct),
                dq_result,
            )],
            """
            table_name STRING,
            run_id STRING,
            dq_ts TIMESTAMP,
            dq_scope STRING,
            total_rows BIGINT,
            pk_null BIGINT,
            monthly_price_usd_null BIGINT,
            seats_included_null BIGINT,
            pk_null_pct DOUBLE,
            monthly_price_usd_null_pct DOUBLE,
            seats_included_null_pct DOUBLE,
            dq_result STRING
            """
        )

        (
            dq_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(cfg["dq_path"])
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {cfg["dq_table"]}
            USING DELTA
            LOCATION '{cfg["dq_path"]}'
        """)

        if dq_result == "FAIL":
            raise ValueError(
                f"DQ FAIL | pk_null_pct={pk_null_pct:.4f} "
                f"| monthly_price_usd_null_pct={monthly_price_usd_null_pct:.4f} "
                f"| seats_included_null_pct={seats_included_null_pct:.4f}"
            )

        # conform (SCD2)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {cfg["conform_table"]} (
                plan_id_sk BIGINT GENERATED BY DEFAULT AS IDENTITY,
                plan_id STRING,
                plan_name STRING,
                monthly_price_usd BIGINT,
                seats_included BIGINT,
                max_units_per_month BIGINT,
                _file_name STRING,
                _source STRING,
                _landing_format STRING,
                silver_effective_start_ts TIMESTAMP,
                silver_effective_end_ts TIMESTAMP,
                updated_at TIMESTAMP,
                etl_run_id STRING,
                record_hash STRING,
                is_current BOOLEAN
            )
            USING DELTA
            LOCATION '{cfg["conform_path"]}'
        """)

        conf_dt = DeltaTable.forName(spark, cfg["conform_table"])

        unknown = spark.range(1).select(
            lit(-1).cast("bigint").alias("plan_id_sk"),
            lit("UNKNOWN").cast("string").alias("plan_id"),
            lit(None).cast("string").alias("plan_name"),
            lit(None).cast("bigint").alias("monthly_price_usd"),
            lit(None).cast("bigint").alias("seats_included"),
            lit(None).cast("bigint").alias("max_units_per_month"),
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

        (
            conf_dt.alias("t")
            .merge(unknown.alias("s"), "t.plan_id_sk = s.plan_id_sk")
            .whenNotMatchedInsertAll()
            .execute()
        )

        scd2_cols = [
            "plan_name",
            "monthly_price_usd",
            "seats_included",
            "max_units_per_month",
            "_file_name",
            "_source",
            "_landing_format",
        ]

        incoming_df = (
            dedup
            .withColumn("silver_effective_start_ts", col("_ingest_ts").cast("timestamp"))
            .withColumn(
                "record_hash",
                sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in scd2_cols]), 256),
            )
            .select(
                "plan_id",
                "plan_name",
                "monthly_price_usd",
                "seats_included",
                "max_units_per_month",
                "_file_name",
                "_source",
                "_landing_format",
                "etl_run_id",
                "record_hash",
                "silver_effective_start_ts",
            )
        )

        conf_active = (
            conf_dt.toDF()
            .filter(col("is_current") == True)
            .select("plan_id", "record_hash")
        )

        joined = incoming_df.alias("inc").join(conf_active.alias("con"), on="plan_id", how="left")

        changed = (
            joined
            .filter(col("con.record_hash").isNotNull() & (col("inc.record_hash") != col("con.record_hash")))
            .select("inc.*")
        )

        new_recs = (
            joined
            .filter(col("con.record_hash").isNull())
            .select("inc.*")
        )

        update_changed = changed.withColumn("merge_key", col(pk)).withColumn("scd_action", lit("UPDATE"))
        insert_changed = changed.withColumn("merge_key", lit(None).cast("string")).withColumn("scd_action", lit("INSERT"))
        insert_new_recs = new_recs.withColumn("merge_key", lit(None).cast("string")).withColumn("scd_action", lit("INSERT"))

        staged = (
            update_changed
            .unionByName(insert_changed)
            .unionByName(insert_new_recs)
            .withColumn("silver_effective_end_ts", lit(None).cast("timestamp"))
            .withColumn("updated_at", current_timestamp())
            .withColumn("is_current", lit(True))
        )

        (
            conf_dt.alias("t")
            .merge(staged.alias("s"), f"t.{pk} = s.merge_key AND t.is_current = true")
            .whenMatchedUpdate(
                condition="t.record_hash <> s.record_hash AND s.scd_action = 'UPDATE'",
                set={
                    "silver_effective_end_ts": "s.silver_effective_start_ts",
                    "updated_at": "current_timestamp()",
                    "is_current": "false",
                    "etl_run_id": "s.etl_run_id",
                },
            )
            .whenNotMatchedInsert(
                condition="s.scd_action = 'INSERT'",
                values={
                    "plan_id": "s.plan_id",
                    "plan_name": "s.plan_name",
                    "monthly_price_usd": "s.monthly_price_usd",
                    "seats_included": "s.seats_included",
                    "max_units_per_month": "s.max_units_per_month",
                    "_file_name": "s._file_name",
                    "_source": "s._source",
                    "_landing_format": "s._landing_format",
                    "silver_effective_start_ts": "s.silver_effective_start_ts",
                    "silver_effective_end_ts": "s.silver_effective_end_ts",
                    "updated_at": "s.updated_at",
                    "etl_run_id": "s.etl_run_id",
                    "record_hash": "s.record_hash",
                    "is_current": "s.is_current",
                },
            )
            .execute()
        )

        upsert_watermark(spark, cfg["state_table"], pipeline_name, dataset, new_wm, run_id)

        spark.sql(f"""
            UPDATE {cfg["run_logs_table"]}
            SET finished_at = current_timestamp(),
                status = 'SUCCESS',
                dq_result = '{dq_result}',
                rows_in = {rows_in},
                rows_out = {rows_out},
                rows_quarantined = {rows_quarantined},
                last_watermark_ts = TIMESTAMP '{new_wm.isoformat(sep=" ")}'
            WHERE pipeline_name = '{pipeline_name}'
              AND dataset = '{dataset}'
              AND run_id = '{run_id}'
        """)

        logger.info(
            "Silver dim_plan SUCCESS | rows_in=%d | rows_out=%d | rows_quarantined=%d",
            rows_in,
            rows_out,
            rows_quarantined,
        )

    except Exception as e:
        msg = str(e).replace("'", "''")
        wm_to_log = last_wm if last_wm is not None else datetime(1900, 1, 1)
        dq_to_log = dq_result if dq_result is not None else "ERROR"

        spark.sql(f"""
            UPDATE {cfg["run_logs_table"]}
            SET finished_at = current_timestamp(),
                status = 'FAILED',
                error_msg = '{msg}',
                rows_in = {rows_in},
                rows_quarantined = {rows_quarantined},
                rows_out = {rows_out},
                dq_result = '{dq_to_log}',
                last_watermark_ts = TIMESTAMP '{wm_to_log.isoformat(sep=" ")}'
            WHERE pipeline_name = '{pipeline_name}'
              AND dataset = '{dataset}'
              AND run_id = '{run_id}'
        """)

        logger.exception("Silver dim_plan FAILED")
        raise