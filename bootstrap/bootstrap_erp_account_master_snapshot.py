import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig

logger = logging.getLogger(__name__)

def _build_config(env: EnvConfig) -> dict[str, str]:
    return {
        "silver_dq_table": f"{env.catalog}.{env.schemas['silver']}.s_dq_erp_account_master_snapshot",
        "silver_quarantine_table": f"{env.catalog}.{env.schemas['silver']}.s_quarantine_erp_account_master_snapshot",
        "silver_conform_table": f"{env.catalog}.{env.schemas['silver']}.s_conform_erp_account_master_snapshot",

        "silver_dq_path": f"{env.silver_base_path}/{env.catalog}/{env.schemas['silver']}/s_erp_account_master_snapshot/s_dq_erp_account_master_snapshot",
        "silver_quarantine_path": f"{env.silver_base_path}/{env.catalog}/{env.schemas['silver']}/s_erp_account_master_snapshot/s_quarantine_erp_account_master_snapshot",
        "silver_conform_path": f"{env.silver_base_path}/{env.catalog}/{env.schemas['silver']}/s_erp_account_master_snapshot/s_conform_erp_account_master_snapshot",
    }


def bootstrap_erp_account_master_snapshot(spark: SparkSession, env: EnvConfig) -> None:

    cfg = _build_config(env=env)

    logger.info("Creating/validating erp_account_master_snapshot in schema %s", f"{env.catalog}.{env.schemas['silver']}")

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
                account_id STRING,
                customer_name STRING,
                email STRING,
                stripe_customer_id STRING,
                plan_code STRING,
                segment STRING,
                country_code STRING,
                region STRING,
                account_created_at_raw DATE,
                status STRING,
                churned_at_raw DATE,
                source_system STRING,
                snapshot_dt DATE,
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
                CREATE TABLE IF NOT EXISTS {cfg["silver_conform_table"]} (
                account_id STRING,
                customer_name STRING,
                email STRING,
                stripe_customer_id STRING,
                plan_code STRING,
                segment STRING,
                country_code STRING,
                region STRING,
                account_created_at_raw DATE,
                status STRING,
                churned_at_raw DATE,
                source_system STRING,
                snapshot_dt DATE,
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