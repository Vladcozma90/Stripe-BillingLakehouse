import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig


logger = logging.getLogger(__name__)

def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "silver_dq_table": f"{env.catalog}.{env.project}_silver.s_dq_stripe_subscription_items",
        "silver_quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_stripe_subscription_items",
        "silver_current_table": f"{env.catalog}.{env.project}_silver.s_current_stripe_subscription_items",
        "gold_table": f"{env.catalog}.{env.project}_gold.g_fact_stripe_subscription_items",

        "silver_dq_path": f"{env.silver_base_path}/{env.project}/stripe_subscription_items/s_dq_stripe_subscription_items",
        "silver_quarantine_path": f"{env.silver_base_path}/{env.project}/stripe_subscription_items/s_quarantine_stripe_subscription_items",
        "silver_current_path": f"{env.silver_base_path}/{env.project}/stripe_subscription_items/s_current_stripe_subscription_items",
        "gold_path": f"{env.gold_base_path}/{env.catalog}/{env.project}/g_fact_stripe_subscription_items",
    }


def bootstrap_subscription_items(spark: SparkSession, env: EnvConfig) -> None:
    
    cfg = _build_config(env=env)

    logger.info("Creating/validating stripe_subscription_items in schema %s", f"{env.catalog}.{env.project}_silver")

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
    logger.info("Ensure table exists %s", f"{cfg["silver_dq_table"]}")


    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["silver_quarantine_table"]} (
                quarantine_reason STRING,
                quarantine_ts TIMESTAMP,
                subscription_item_id STRING,
                subscription_id STRING,
                price_id STRING,
                product_id STRING,
                item_currency STRING,
                billing_interval STRING,
                price_type STRING,
                usage_type STRING,
                quantity BIGINT,
                unit_amount BIGINT,
                item_created_ts TIMESTAMP,
                item_current_period_start_ts TIMESTAMP,
                item_current_period_end_ts TIMESTAMP,
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
                subscription_item_id STRING,
                subscription_id STRING,
                price_id STRING,
                product_id STRING,
                item_currency STRING,
                billing_interval STRING,
                price_type STRING,
                usage_type STRING,
                quantity BIGINT,
                unit_amount BIGINT,
                item_created_ts TIMESTAMP,
                item_current_period_start_ts TIMESTAMP,
                item_current_period_end_ts TIMESTAMP,
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