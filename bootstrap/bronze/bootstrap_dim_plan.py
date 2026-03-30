import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig

logger = logging.getLogger(__name__)


def bootstrap_dim_plan(spark: SparkSession, env: EnvConfig) -> None:
    cfg = {
        "bronze_dim_plan_table": f"{env.raw_base_path}.{env.project}_bronze/dim_plan.",
        "bronze_dim_plan_path": f"{env.raw_base_path}/{env.project}/dim_plan",
    }
    logger.info("Creating/validating OPS tables in schema %s", f"{env.catalog}.{env.project}_bronze")


    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {cfg["bronze_dim_plan_table"]} (
                plan_id STRING,
                plan_name STRING,
                monthly_price_usd INTEGER,
                seats_included INTEGER,
                max_units_per_month BIGINT
                )
                USING DELTA
                LOCATION '{cfg["bronze_dim_plan_path"]}'
            """)
    logger.info("Ensure table exists: %s", f"{cfg["bronze_dim_plan_table"]}")