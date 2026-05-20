import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig

logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "silver_dq_table": f"{env.catalog}.{env.schemas['silver']}.s_dq_erp_usage_daily",
        "silver_quarantine_table": f"{env.catalog}.{env.schemas['silver']}.s_quarantine_erp_usage_daily",
        "silver_current_table": f"{env.catalog}.{env.schemas['silver']}.s_current_erp_usage_daily",
        "gold_table": f"{env.catalog}.{env.schemas['gold']}.g_fact_usage_daily",

        "silver_dq_path": f"{env.silver_base_path}/{env.catalog}/{env.schemas['silver']}/s_erp_usage_daily/s_dq_erp_usage_daily",
        "silver_quarantine_path": f"{env.silver_base_path}/{env.catalog}/{env.schemas['silver']}/s_erp_usage_daily/s_quarantine_erp_usage_daily",
        "silver_current_path": f"{env.silver_base_path}/{env.catalog}/{env.schemas['silver']}/s_erp_usage_daily/s_current_erp_usage_daily",
        "gold_path": f"{env.gold_base_path}/{env.catalog}/{env.schemas['gold']}/g_fact_usage_daily",
    }


def bootstrap_erp_usage_daily(spark: SparkSession, env: EnvConfig) -> None:

    cfg = _build_config(env=env)

    logger.info("Creating/validating erp_plan_catalog in schema %s", f"{env.catalog}.{env.schemas['silver']}")
    
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


    logger.info("Creating/validating erp_usage_daily in schema %s", f"{env.catalog}.{env.schemas['gold']}")
    
    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["gold_fact_table"]} (
                    invoice_business_key STRING NOT NULL,
                    invoice_id STRING,

                    subscription_sk BIGINT,
                    customer_sk BIGINT,
                    plan_sk BIGINT,

                    stripe_customer_id STRING,
                    subscription_id STRING,
                    account_id STRING,
                    plan_code STRING,

                    invoice_status STRING,
                    collection_method STRING,
                    currency STRING,
                    invoice_number STRING,

                    amount_due BIGINT,
                    amount_paid BIGINT,
                    amount_remaining BIGINT,
                    subtotal BIGINT,
                    subtotal_excluding_tax BIGINT,
                    total BIGINT,

                    attempt_count BIGINT,
                    is_attempted BOOLEAN,
                    is_livemode BOOLEAN,
                    auto_advance BOOLEAN,

                    created_ts TIMESTAMP,
                    due_date_ts TIMESTAMP,
                    period_start_ts TIMESTAMP,
                    period_end_ts TIMESTAMP,
                    status_finalized_ts TIMESTAMP,
                    status_paid_ts TIMESTAMP,
                    status_voided_ts TIMESTAMP,
                    status_marked_uncollectible_ts TIMESTAMP,
                    api_extracted_ts TIMESTAMP,

                    etl_run_id STRING,
                    gold_processed_ts TIMESTAMP,
                    gold_processed_date DATE,
                    record_hash STRING
                )
                USING DELTA
                LOCATION '{cfg["gold_fact_path"]}'
                """)



