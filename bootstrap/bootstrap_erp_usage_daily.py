import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig

logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "bronze_table": f"{env.catalog}.{env.project}_bronze.b_erp_usage_daily",
        "silver_dq_table": f"{env.catalog}.{env.project}_silver.s_dq_erp_usage_daily",
        "silver_quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_erp_usage_daily",
        "silver_current_table": f"{env.catalog}.{env.project}_silver.s_current_erp_usage_daily",

        "bronze_path": f"{env.raw_base_path}/{env.project}/b_erp_usage_daily",
        "silver_dq_path": f"{env.curated_base_path}/{env.project}/erp_usage_daily/s_dq_erp_usage_daily",
        "silver_quarantine_path": f"{env.curated_base_path}/{env.project}/erp_usage_daily/s_quarantine_erp_usage_daily",
        "silver_current_path": f"{env.curated_base_path}/{env.project}/erp_usage_daily/s_current_erp_usage_daily",
    }


def bootstrap_erp_usage_daily(spark: SparkSession, env: EnvConfig) -> None:

    cfg = _build_config(env=env)

    logger.info("Creating/validating erp_plan_catalog in schema %s", f"{env.catalog}.{env.project}_bronze")

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["bronze_table"]} (
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
                _landing_format STRING
                )
                USING DELTA
                LOCATION '{cfg["bronze_path"]}'
            """)
    logger.info("Ensure table exists: %s", f"{cfg["bronze_table"]}")

    
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



