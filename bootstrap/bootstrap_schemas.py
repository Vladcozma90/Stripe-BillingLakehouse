import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig


logger = logging.getLogger(__name__)

def ensure_schemas(spark: SparkSession, env: EnvConfig) -> None:

    spark.sql(f"USE CATALOG {env.catalog}")

    schemas = [
        f"{env.project}_ops",
        f"{env.project}_bronze",
        f"{env.project}_silver",
        f"{env.project}_gold",
    ]

    for s in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env.catalog}.{s}")
        logger.info("Ensure schema exists: %s", f"{env.catalog}.{s}")