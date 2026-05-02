import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig


logger = logging.getLogger(__name__)

def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "silver_dq_table": f"{env.catalog}.{env.project}_silver.s_dq_stripe_customers",
        "silver_quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_stripe_customers",
        "silver_conform_table": f"{env.catalog}.{env.project}_silver.s_conform_stripe_customers",
        "gold_table": f"{env.catalog}.{env.project}_gold.g_dim_stripe_customers",
        
        "silver_dq_path": f"{env.silver_base_path}/{env.catalog}/{env.project}/s_stripe_customers/s_dq_stripe_customers",
        "silver_quarantine_path": f"{env.silver_base_path}/{env.catalog}/{env.project}/s_stripe_customers/s_quarantine_stripe_customers",
        "silver_conform_path": f"{env.silver_base_path}/{env.catalog}/{env.project}/s_stripe_customers/s_conform_stripe_customers",
        "gold_path": f"{env.gold_base_path}/{env.catalog}/{env.project}/g_dim_stripe_customers"
    }


def bootstrap_stripe_customers(spark: SparkSession, env: EnvConfig) -> None:

    cfg = _build_config(env=env)

    logger.info("Creating/validating stripe_customers in schema %s", f"{env.catalog}.{env.project}_silver")
    
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
                stripe_customer_id STRING,
                email STRING,
                currency STRING,
                description STRING,
                order_id STRING,
                address STRING,
                customer_created_ts TIMESTAMP,
                is_delinquent BOOLEAN,
                livemode BOOLEAN,
                api_extracted_ts TIMESTAMP,
                _ingest_ts TIMESTAMP,
                _ingest_date DATE,
                _file_name STRING,
                _source_format STRING,
                etl_run_id STRING,
                silver_processed_ts TIMESTAMP,
                silver_processed_date DATE
                )
                USING DELTA
                LOCATION '{cfg["silver_quarantine_path"]}'
            """)
    logger.info("Ensure table exists: %s", f"{cfg["silver_quarantine_table"]}")

    
    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["silver_conform_table"]} (
                stripe_customers_sk BIGINT GENERATED ALWAYS AS IDENTITY,
                stripe_customer_id STRING,
                email STRING,
                currency STRING,
                description STRING,
                order_id STRING,
                address STRING,
                customer_created_ts TIMESTAMP,
                is_delinquent BOOLEAN,
                livemode BOOLEAN,
                api_extracted_ts TIMESTAMP,
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