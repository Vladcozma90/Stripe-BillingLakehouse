from __future__ import annotations

import uuid
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    current_timestamp,
    current_date,
)

from services.envs import EnvConfig
from services.audit import (
    insert_run_log_start,
    update_run_log_no_new_data,
    update_run_log_success,
    update_run_log_failure,
)
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "silver_conform_table": f"{env.catalog}.{env.project}_silver.s_conform_stripe_customers",
        "gold_table": f"{env.catalog}.{env.project}_gold.g_dim_customer",
        "gold_path": f"{env.curated_base_path}/{env.project}/stripe_customers/g_dim_customer",
    }


def _get_required_columns() -> list[str]:
    return [
        "stripe_customers_sk",
        "customer_id",
        "email",
        "customer_name",
        "currency",
        "description",
        "country_code",
        "customer_created_ts",
        "is_delinquent",
        "is_livemode",
        "api_extracted_ts",
        "silver_effective_start_ts",
        "silver_effective_end_ts",
        "is_current",
        "etl_run_id",
    ]


def _build_gold_dim_customer(silver_conform_df: DataFrame) -> DataFrame:
    return (
        silver_conform_df
        .withColumn("customer_id", trim(col("customer_id")).cast("string"))
        .withColumn("email", trim(col("email")).cast("string"))
        .withColumn("customer_name", trim(col("customer_name")).cast("string"))
        .withColumn("currency", lower(trim(col("currency"))).cast("string"))
        .withColumn("description", trim(col("description")).cast("string"))
        .withColumn("country_code", upper(trim(col("country_code"))).cast("string"))
        .withColumn("customer_created_ts", col("customer_created_ts").cast("timestamp"))
        .withColumn("is_delinquent", col("is_delinquent").cast("boolean"))
        .withColumn("is_livemode", col("is_livemode").cast("boolean"))
        .withColumn("api_extracted_ts", col("api_extracted_ts").cast("timestamp"))
        .withColumn("gold_loaded_ts", current_timestamp())
        .withColumn("gold_loaded_date", current_date())
        .withColumn("etl_run_id", col("etl_run_id").cast("string"))
        .select(
            "stripe_customers_sk",
            "customer_id",
            "email",
            "customer_name",
            "currency",
            "description",
            "country_code",
            "customer_created_ts",
            "is_delinquent",
            "is_livemode",
            "api_extracted_ts",
            "silver_effective_start_ts",
            "silver_effective_end_ts",
            "is_current",
            "gold_loaded_ts",
            "gold_loaded_date",
            "etl_run_id",
        )
    )


def run_gold_dim_customers(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "gold_dim_stripe_customers"
    dataset = "dim_stripe_customers"
    run_id = uuid.uuid4().hex

    cfg = _build_config(env)

    rows_in = 0
    rows_out = 0

    insert_run_log_start(
        spark=spark,
        run_logs_table=cfg["run_logs_table"],
        pipeline_name=pipeline_name,
        dataset=dataset,
        target_table=cfg["gold_table"],
        run_id=run_id,
    )

    try:
        logger.info("Gold dim_customer start | run_id=%s", run_id)

        silver_conform_df = spark.table(cfg["silver_conform_table"])

        required_columns = _get_required_columns()
        missing_columns = [c for c in required_columns if c not in silver_conform_df.columns]
        if missing_columns:
            raise ValueError(f"Silver conform missing required cols: {missing_columns}")

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

        gold_df = _build_gold_dim_customer(
            silver_conform_df=silver_conform_df,
            run_id=run_id,
        )

        rows_in = silver_conform_df.count()
        rows_out = gold_df.count()

        gold_dt = DeltaTable.forName(spark, cfg["gold_table"])

        (
            gold_dt.alias("t")
            .merge(
                gold_df.alias("s"),
                "t.stripe_customers_sk = s.stripe_customers_sk",
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
            "Gold dim_customer SUCCESS | rows_in=%d | rows_out=%d",
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
        logger.exception("Gold dim_customer FAILED")
        raise