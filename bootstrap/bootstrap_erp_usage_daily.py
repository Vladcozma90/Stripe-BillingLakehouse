import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig

logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "silver_dq_table": f"{env.catalog}.{env.project}_silver.s_dq_erp_usage_daily",
        "silver_quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_erp_usage_daily",
        "silver_current_table": f"{env.catalog}.{env.project}_silver.s_current_erp_usage_daily",
        "gold_table": f"{env.catalog}.{env.project}_gold.g_fact_usage_daily",

        "silver_dq_path": f"{env.silver_base_path}/{env.catalog}/{env.project}/s_erp_usage_daily/s_dq_erp_usage_daily",
        "silver_quarantine_path": f"{env.silver_base_path}/{env.project}/s_erp_usage_daily/s_quarantine_erp_usage_daily",
        "silver_current_path": f"{env.silver_base_path}/{env.project}/s_erp_usage_daily/s_current_erp_usage_daily",
        "gold_path": f"{env.gold_base_path}/{env.catalog}/{env.project}/g_fact_usage_daily",
    }


def bootstrap_erp_usage_daily(spark: SparkSession, env: EnvConfig) -> None:

    cfg = _build_config(env=env)

    logger.info("Creating/validating erp_plan_catalog in schema %s", f"{env.catalog}.{env.project}_silver")
    
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
                usage_id STRING,
                event_ts TIMESTAMP,
                usage_date DATE,
                account_id STRING,
                feature_code STRING,
                active_users BIGINT,
                units_raw BIGINT,
                source_system STRING,
                batch_id STRING,
                _ingest_ts TIMESTAMP,
                _ingest_date DATE,
                _file_name STRING,
                _source STRING,
                _landing_format STRING,
                etl_run_id STRING,
                silver_processed_ts TIMESTAMP,
                silver_processed_date DATE
                )
                USING DELTA
                LOCATION '{cfg["silver_quarantine_path"]}'
            """)
    logger.info("Ensure table exists: %s", f"{cfg["silver_quarantine_table"]}")


    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["silver_current_table"]} (
                usage_id STRING,
                event_ts TIMESTAMP,
                usage_date DATE,
                account_id STRING,
                feature_code STRING,
                active_users BIGINT,
                units_raw BIGINT,
                source_system STRING,
                batch_id STRING,
                etl_run_id STRING,
                silver_processed_ts TIMESTAMP,
                silver_processed_date DATE,
                _file_name STRING,
                _source STRING,
                _landing_format STRING
                )
                USING DELTA
                LOCATION '{cfg["silver_current_path"]}'
            """)
    logger.info("Ensure table exists: %s", f"{cfg["silver_current_table"]}")


    logger.info("Creating/validating erp_usage_daily in schema %s", f"{env.catalog}.{env.project}_gold")

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["gold_table"]} (
                usage_id STRING,
                account_master_snapshot_sk BIGINT,
                plan_catalog_sk BIGINT,
                event_ts TIMESTAMP,
                f.usage_date DATE,
                account_id STRING,
                feature_code STRING,
                active_users BIGINT,
                units_raw BIGINT,
                source_system STRING,
                batch_id STRING,
                _file_name STRING,
                _source STRING,
                _landing_format STRING,
                etl_run_id STRING,
                gold_loaded_ts TIMESTAMP,
                gold_loaded_date DATE
                )
                USING DELTA
                LOCATION '{cfg["gold_path"]}'
            """)



