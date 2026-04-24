import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig



logger = logging.getLogger(__name__)


def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
        "state_table": f"{env.catalog}.{env.project}_ops.pipeline_state",
        "silver_dq_table": f"{env.catalog}.{env.project}_silver.s_dq_stripe_invoices",
        "silver_quarantine_table": f"{env.catalog}.{env.project}_silver.s_quarantine_stripe_invoices",
        "silver_current_table": f"{env.catalog}.{env.project}_silver.s_current_stripe_invoices",

        "silver_dq_path": f"{env.curated_base_path}/{env.project}/stripe_invoices/s_dq_stripe_invoices",
        "silver_quarantine_path": f"{env.curated_base_path}/{env.project}/stripe_invoices/s_quarantine_stripe_invoices",
        "silver_current_path": f"{env.curated_base_path}/{env.project}/stripe_invoices/s_current_stripe_invoices",
    }

def bootstrap_stripe_invoices(spark: SparkSession, env: EnvConfig) -> None:

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
                invoice_id STRING,
                stripe_customer_id STRING,
                subscription_id STRING,
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
    logger.info("Ensure table exists %s", f"{cfg["silver_quarantine_table"]}")

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["silver_current_table"]} (
                invoice_id STRING,
                stripe_customer_id STRING,
                subscription_id STRING,
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
                LOCATION '{cfg["silver_current_path"]}'
            """)
    logger.info("Ensure table exists %s", f"{cfg["silver_current_table"]}")

