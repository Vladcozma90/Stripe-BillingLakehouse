import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig

logger = logging.getLogger(__name__)

def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "bronze_table": f"{env.catalog}.{env.project}_bronze.b_erp_plan_catalog",
        "silver_dq_table": f"{env.catalog}.{env.project}_silver.s_dq_erp_plan_catalog",
        "silver_quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_erp_plan_catalog",
        "silver_current_table": f"{env.catalog}.{env.project}_silver.s_current_erp_plan_catalog",
        "silver_conform_table": f"{env.catalog}.{env.project}_silver.s_conform_erp_plan_catalog",

        "bronze_path": f"{env.raw_base_path}/{env.project}/b_erp_plan_catalog",
        "silver_dq_path": f"{env.curated_base_path}/{env.project}/s_erp_plan_catalog/s_dq_erp_plan_catalog",
        "silver_quarantine_path": f"{env.curated_base_path}/{env.project}/s_erp_plan_catalog/s_quarantine_erp_plan_catalog",
        "silver_current_path": f"{env.curated_base_path}/{env.project}/s_erp_plan_catalog/s_current_erp_plan_catalog",
        "silver_conform_path": f"{env.curated_base_path}/{env.project}/s_erp_plan_catalog/s_conform_erp_plan_catalog",
    }


def bootstrap_erp_plan_catalog(spark: SparkSession, env: EnvConfig) -> None:

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
                plan_code STRING,
                plan_name STRING,
                monthly_price_usd INTEGER,
                seats_included INTEGER,
                max_units_per_month BIGINT,
                currency STRING,
                billing_period STRING,
                effective_from DATE,
                effective_to DATE,
                source_is_current BOOLEAN,
                price_version STRING,
                _ingest_ts TIMESTAMP,
                _ingest_date DATE,
                _file_name STRING,
                _source STRING,
                _landing_format STRING,
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
                plan_code STRING,
                plan_name STRING,
                monthly_price_usd INTEGER,
                seats_included INTEGER,
                max_units_per_month BIGINT,
                currency STRING,
                billing_period STRING,
                effective_from DATE,
                effective_to DATE,
                source_is_current BOOLEAN,
                price_version STRING,
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


    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["silver_conform_table"]} (
                    plan_catalog_sk BIGINT GENERATED ALWAYS AS IDENTITY,
                    plan_code STRING,
                    plan_name STRING,
                    monthly_price_usd BIGINT,
                    seats_included BIGINT,
                    max_units_per_month BIGINT,
                    curreny BIGINT,
                    billing_period STRING,
                    effective_from DATE,
                    effective_to DATE,
                    source_is_current BOOLEAN,
                    price_version STRING,
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
                LOCATION '{cfg["silver_conform_path"]}'
            """)
    logger.info("Ensure table exists: %s", f"{cfg["silver_conform_table"]}")