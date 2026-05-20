import uuid
import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    coalesce,
    current_timestamp,
    current_date,
    sha2,
    concat_ws,
)

from src.services.envs import EnvConfig
from src.services.audit import (
    insert_run_log_start,
    update_run_log_success,
    update_run_log_failure,
)

logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.schemas['ops']}.run_logs",

        "silver_account_table": f"{env.catalog}.{env.schemas['silver']}.s_conform_erp_account_master_snapshot",
        "silver_stripe_customer_table": f"{env.catalog}.{env.schemas['silver']}.s_conform_stripe_customers",

        "gold_customer_table": f"{env.catalog}.{env.schemas['gold']}.g_dim_customers",
        "gold_customer_path": f"{env.gold_base_path}/{env.catalog}/{env.schemas['gold']}/g_dim_customers",
    }


def _get_required_account_columns() -> list[str]:
    return [
        "account_id",
        "stripe_customer_id",
        "customer_name",
        "email",
        "plan_code",
        "segment",
        "country_code",
        "region",
        "account_created_at_raw",
        "status",
        "churned_at_raw",
        "source_system",
        "snapshot_dt",
        "silver_effective_start_ts",
        "silver_effective_end_ts",
        "is_current",
    ]


def _get_required_stripe_customer_columns() -> list[str]:
    return [
        "stripe_customer_id",
        "email",
        "currency",
        "description",
        "order_id",
        "customer_created_ts",
        "is_delinquent",
        "is_livemode",
        "api_extracted_ts",
        "silver_effective_start_ts",
        "silver_effective_end_ts",
        "is_current",
    ]


def _validate_required_columns(
    df: DataFrame,
    required_columns: list[str],
    table_name: str,
) -> None:
    missing_columns = [c for c in required_columns if c not in df.columns]

    if missing_columns:
        raise ValueError(f"{table_name} missing required columns: {missing_columns}")


def _build_gold_dim_customer(
    account_df: DataFrame,
    stripe_customer_df: DataFrame,
    run_id: str,
) -> DataFrame:
    account_current_df = (
        account_df
        .filter(col("is_current") == lit(True))
        .filter(col("account_id") != lit("UNKNOWN"))
        .alias("a")
    )

    stripe_customer_current_df = (
        stripe_customer_df
        .filter(col("is_current") == lit(True))
        .filter(col("stripe_customer_id") != lit("UNKNOWN"))
        .alias("s")
    )

    joined_df = (
        account_current_df
        .join(
            stripe_customer_current_df,
            on=col("a.stripe_customer_id") == col("s.stripe_customer_id"),
            how="left",
        )
    )

    dim_df = (
        joined_df
        .select(
            col("a.account_id").cast("string").alias("account_id"),
            col("a.stripe_customer_id").cast("string").alias("stripe_customer_id"),

            col("a.customer_name").cast("string").alias("customer_name"),
            coalesce(col("a.email"), col("s.email")).cast("string").alias("email"),

            col("a.plan_code").cast("string").alias("plan_code"),
            col("a.segment").cast("string").alias("segment"),
            col("a.country_code").cast("string").alias("country_code"),
            col("a.region").cast("string").alias("region"),

            col("a.account_created_at_raw").cast("date").alias("account_created_date"),
            col("a.status").cast("string").alias("account_status"),
            col("a.churned_at_raw").cast("date").alias("churned_date"),
            col("a.source_system").cast("string").alias("account_source_system"),
            col("a.snapshot_dt").cast("date").alias("account_snapshot_date"),

            col("s.currency").cast("string").alias("stripe_currency"),
            col("s.description").cast("string").alias("stripe_description"),
            col("s.order_id").cast("string").alias("stripe_order_id"),
            col("s.customer_created_ts").cast("timestamp").alias("stripe_customer_created_ts"),
            col("s.is_delinquent").cast("boolean").alias("is_delinquent"),
            col("s.is_livemode").cast("boolean").alias("is_livemode"),
            col("s.api_extracted_ts").cast("timestamp").alias("stripe_api_extracted_ts"),

            col("a.silver_effective_start_ts").cast("timestamp").alias("account_silver_effective_start_ts"),
            col("a.silver_effective_end_ts").cast("timestamp").alias("account_silver_effective_end_ts"),
            col("s.silver_effective_start_ts").cast("timestamp").alias("stripe_silver_effective_start_ts"),
            col("s.silver_effective_end_ts").cast("timestamp").alias("stripe_silver_effective_end_ts"),

            lit(run_id).cast("string").alias("etl_run_id"),
            current_timestamp().alias("gold_processed_ts"),
            current_date().alias("gold_processed_date"),
        )
    )

    return (
        dim_df
        .withColumn(
            "customer_business_key",
            coalesce(col("account_id"), col("stripe_customer_id"))
        )
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("account_id").cast("string"), lit("")),
                    coalesce(col("stripe_customer_id").cast("string"), lit("")),
                    coalesce(col("customer_name").cast("string"), lit("")),
                    coalesce(col("email").cast("string"), lit("")),
                    coalesce(col("plan_code").cast("string"), lit("")),
                    coalesce(col("segment").cast("string"), lit("")),
                    coalesce(col("country_code").cast("string"), lit("")),
                    coalesce(col("region").cast("string"), lit("")),
                    coalesce(col("account_created_date").cast("string"), lit("")),
                    coalesce(col("account_status").cast("string"), lit("")),
                    coalesce(col("churned_date").cast("string"), lit("")),
                    coalesce(col("account_source_system").cast("string"), lit("")),
                    coalesce(col("account_snapshot_date").cast("string"), lit("")),
                    coalesce(col("stripe_currency").cast("string"), lit("")),
                    coalesce(col("stripe_description").cast("string"), lit("")),
                    coalesce(col("stripe_order_id").cast("string"), lit("")),
                    coalesce(col("stripe_customer_created_ts").cast("string"), lit("")),
                    coalesce(col("is_delinquent").cast("string"), lit("")),
                    coalesce(col("is_livemode").cast("string"), lit("")),
                    coalesce(col("stripe_api_extracted_ts").cast("string"), lit("")),
                ),
                256,
            )
        )
        .select(
            "customer_business_key",
            "account_id",
            "stripe_customer_id",
            "customer_name",
            "email",
            "plan_code",
            "segment",
            "country_code",
            "region",
            "account_created_date",
            "account_status",
            "churned_date",
            "account_source_system",
            "account_snapshot_date",
            "stripe_currency",
            "stripe_description",
            "stripe_order_id",
            "stripe_customer_created_ts",
            "is_delinquent",
            "is_livemode",
            "stripe_api_extracted_ts",
            "account_silver_effective_start_ts",
            "account_silver_effective_end_ts",
            "stripe_silver_effective_start_ts",
            "stripe_silver_effective_end_ts",
            "etl_run_id",
            "gold_processed_ts",
            "gold_processed_date",
            "record_hash",
        )
    )


def _merge_gold_dim_customer(
    spark: SparkSession,
    target_table: str,
    source_df: DataFrame,
) -> None:
    target_dt = DeltaTable.forName(spark, target_table)

    merge_condition = "t.customer_business_key <=> s.customer_business_key"

    (
        target_dt.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition="t.record_hash <> s.record_hash",
            set={
                "account_id": "s.account_id",
                "stripe_customer_id": "s.stripe_customer_id",
                "customer_name": "s.customer_name",
                "email": "s.email",
                "plan_code": "s.plan_code",
                "segment": "s.segment",
                "country_code": "s.country_code",
                "region": "s.region",
                "account_created_date": "s.account_created_date",
                "account_status": "s.account_status",
                "churned_date": "s.churned_date",
                "account_source_system": "s.account_source_system",
                "account_snapshot_date": "s.account_snapshot_date",
                "stripe_currency": "s.stripe_currency",
                "stripe_description": "s.stripe_description",
                "stripe_order_id": "s.stripe_order_id",
                "stripe_customer_created_ts": "s.stripe_customer_created_ts",
                "is_delinquent": "s.is_delinquent",
                "is_livemode": "s.is_livemode",
                "stripe_api_extracted_ts": "s.stripe_api_extracted_ts",
                "account_silver_effective_start_ts": "s.account_silver_effective_start_ts",
                "account_silver_effective_end_ts": "s.account_silver_effective_end_ts",
                "stripe_silver_effective_start_ts": "s.stripe_silver_effective_start_ts",
                "stripe_silver_effective_end_ts": "s.stripe_silver_effective_end_ts",
                "etl_run_id": "s.etl_run_id",
                "gold_processed_ts": "s.gold_processed_ts",
                "gold_processed_date": "s.gold_processed_date",
                "record_hash": "s.record_hash",
            },
        )
        .whenNotMatchedInsert(
            values={
                "customer_business_key": "s.customer_business_key",
                "account_id": "s.account_id",
                "stripe_customer_id": "s.stripe_customer_id",
                "customer_name": "s.customer_name",
                "email": "s.email",
                "plan_code": "s.plan_code",
                "segment": "s.segment",
                "country_code": "s.country_code",
                "region": "s.region",
                "account_created_date": "s.account_created_date",
                "account_status": "s.account_status",
                "churned_date": "s.churned_date",
                "account_source_system": "s.account_source_system",
                "account_snapshot_date": "s.account_snapshot_date",
                "stripe_currency": "s.stripe_currency",
                "stripe_description": "s.stripe_description",
                "stripe_order_id": "s.stripe_order_id",
                "stripe_customer_created_ts": "s.stripe_customer_created_ts",
                "is_delinquent": "s.is_delinquent",
                "is_livemode": "s.is_livemode",
                "stripe_api_extracted_ts": "s.stripe_api_extracted_ts",
                "account_silver_effective_start_ts": "s.account_silver_effective_start_ts",
                "account_silver_effective_end_ts": "s.account_silver_effective_end_ts",
                "stripe_silver_effective_start_ts": "s.stripe_silver_effective_start_ts",
                "stripe_silver_effective_end_ts": "s.stripe_silver_effective_end_ts",
                "etl_run_id": "s.etl_run_id",
                "gold_processed_ts": "s.gold_processed_ts",
                "gold_processed_date": "s.gold_processed_date",
                "record_hash": "s.record_hash",
            },
        )
        .execute()
    )


def run_gold_dim_customers(spark: SparkSession, env: EnvConfig) -> None:
    pipeline_name = "gold_dim_customers"
    dataset = "dim_customers"
    run_id = uuid.uuid4().hex

    cfg = _build_config(env)

    rows_in = 0
    rows_out = 0
    rows_quarantined = 0
    dq_result = "OK"

    dim_customer_df = None

    insert_run_log_start(
        spark=spark,
        run_logs_table=cfg["run_logs_table"],
        pipeline_name=pipeline_name,
        dataset=dataset,
        target_table=cfg["gold_customer_table"],
        run_id=run_id,
    )

    try:
        logger.info("Gold dim_customer start | run_id=%s", run_id)

        account_df = spark.table(cfg["silver_account_table"])
        stripe_customer_df = spark.table(cfg["silver_stripe_customer_table"])

        _validate_required_columns(
            df=account_df,
            required_columns=_get_required_account_columns(),
            table_name=cfg["silver_account_table"],
        )

        _validate_required_columns(
            df=stripe_customer_df,
            required_columns=_get_required_stripe_customer_columns(),
            table_name=cfg["silver_stripe_customer_table"],
        )

        account_current_count = account_df.filter(col("is_current") == lit(True)).count()
        stripe_customer_current_count = stripe_customer_df.filter(col("is_current") == lit(True)).count()
        rows_in = account_current_count + stripe_customer_current_count

        dim_customer_df = _build_gold_dim_customer(
            account_df=account_df,
            stripe_customer_df=stripe_customer_df,
            run_id=run_id,
        ).persist()

        rows_out = dim_customer_df.count()

        if rows_out == 0:
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
                last_watermark_ts=None,
            )

            logger.info("No customer rows to merge | run_id=%s", run_id)
            return

        _merge_gold_dim_customer(
            spark=spark,
            target_table=cfg["gold_customer_table"],
            source_df=dim_customer_df,
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
            rows_quarantined=rows_quarantined,
            dq_result=dq_result,
            last_watermark_ts=None,
        )

        logger.exception("Gold dim_customer FAILED | run_id=%s", run_id)
        raise

    finally:
        if dim_customer_df is not None:
            try:
                dim_customer_df.unpersist()
            except Exception as e:
                logger.warning("Failed to unpersist dim_customer_df: %s | run_id=%s", e, run_id)