import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig


logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "bronze_table": f"{env.catalog}.{env.project}_bronze.b_stripe_subscriptions",
        "silver_dq_table": f"{env.catalog}.{env.project}_silver.s_dq_stripe_subscriptions",
        "silver_quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_stripe_subscriptions",
        "silver_current_table": f"{env.catalog}.{env.project}_silver.s_current_stripe_subscriptions",
        "silver_conform_table": f"{env.catalog}.{env.project}_silver.s_conform_stripe_subscriptions",

        "bronze_path": f"{env.catalog}/{env.project}/b_stripe_subscriptions",
        "silver_dq_path": f"{env.catalog}/{env.project}/stripe_subscriptions/s_dq_stripe_subscriptions",
        "silver_quarantine_path": f"{env.catalog}/{env.project}/stripe_subscriptions/s_quarantine_stripe_subscriptions",
        "silver_current_path": f"{env.catalog}/{env.project}/stripe_subscriptions/s_current_stripe_subscriptions",
        "silver_conform_path": f"{env.catalog}/{env.project}/stripe_subscriptions/s_conform_stripe_subscriptions",
    }


def bootstrap_stripe_subscriptions(spark: SparkSession, env: EnvConfig) -> None:

    cfg = _build_config(env=env)

    logger.info("Creating/validating stripe_subscriptions in schema %s", f"{env.catalog}.{env.project}_silver")

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["silver_dq_table"]} (
                dq_source_table STRING,
                run_id STRING,
                dq_ts TIMESTAMP,
                total_rows BIGINT,
                column_name STRING,
                rule_name STRING,
                acutal_value DOUBLE,
                threshold_value DOUBLE,
                failed_rows BIGINT,
                severity STRING,
                dq_result STRING
                )
                USING DELTA
                LOCATION '{cfg["silver_dq_path"]}'
            """)
    logger.info("Ensure table exists: %s", f"{cfg["silver_dq_table"]}")

    
    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["silver_quarantine_table"]} (
                quarantine_reason STRING,
                quarantine_ts TIMESTAMP,
                subscription_id STRING,
                stripe_customer_id STRING,
                subscription_status STRING,
                collection_method STRING,
                currency STRING,
                latest_invoice_id STRING,
                created_ts TIMESTAMP,
                start_date_ts TIMESTAMP,
                billing_cycle_anchor_ts TIMESTAMP,
                cancel_at_ts TIMESTAMP,
                canceled_at_ts TIMESTAMP,
                trial_start_ts TIMESTAMP,
                trial_end_ts TIMESTAMP,
                cancel_at_period_end BOOLEAN,
                livemode BOOLEAN,
                api_extracted_ts TIMESTAMP,
                _ingest_ts TIMESTAMP,
                _ingest_date DATE,
                _file_name STRING,
                _source_format STRING,
                etl_run_id STRING,
                silver_processed_ts TIMESTAMP,
                silver_processed_date DATE,
                )
                USING DELTA
                LOCATION '{cfg["silver_quarantine_path"]}'
            """)
    logger.info("Ensure table exists: %s", f"{cfg["silver_quarantine_table"]}")
    
    
    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["silver_current_table"]} (
                subscription_id STRING,
                stripe_customer_id STRING,
                subscription_status STRING,
                collection_method STRING,
                currency STRING,
                latest_invoice_id STRING,
                created_ts TIMESTAMP,
                start_date_ts TIMESTAMP,
                billing_cycle_anchor_ts TIMESTAMP,
                cancel_at_ts TIMESTAMP,
                canceled_at_ts TIMESTAMP,
                trial_start_ts TIMESTAMP,
                trial_end_ts TIMESTAMP,
                cancel_at_period_end BOOLEAN,
                livemode BOOLEAN,
                api_extracted_ts TIMESTAMP,
                _ingest_ts TIMESTAMP,
                _ingest_date DATE,
                _file_name STRING,
                _source_format STRING,
                etl_run_id STRING,
                silver_processed_ts TIMESTAMP,
                silver_processed_date DATE,
                )
                USING DELTA
                LOCATION '{cfg["silver_current_path"]}'
            """)
    logger.info("Ensure table exists: %s", f"{cfg["silver_current_table"]}")
    

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["silver_conform_table"]} (
                stripe_subscriptions_sk BIGINT GENERATED ALWAYS AS IDENTITY,
                subscription_id STRING,
                stripe_customer_id STRING,
                subscription_status STRING,
                collection_method STRING,
                currency STRING,
                latest_invoice_id STRING,
                created_ts TIMESTAMP,
                start_date_ts TIMESTAMP,
                billing_cycle_anchor_ts TIMESTAMP,
                cancel_at_ts TIMESTAMP,
                canceled_at_ts TIMESTAMP,
                trial_start_ts TIMESTAMP,
                trial_end_ts TIMESTAMP,
                cancel_at_period_end BOOLEAN,
                livemode BOOLEAN,
                api_extracted_ts TIMESTAMP,
                _ingest_ts TIMESTAMP,
                _ingest_date DATE,
                _file_name STRING,
                _source_format STRING,
                etl_run_id STRING,
                silver_effective_start_ts TIMESTAMP,
                silver_effective_end_ts TIMESTAMP,
                record_hash STRING,
                is_current BOOLEAN
                )
                USING DELTA
                LOCATION '{cfg["silver_conform_path"]}'
            """)
    logger.info("Ensure table exists: %s", f"{cfg["silver_conform_table"]}")