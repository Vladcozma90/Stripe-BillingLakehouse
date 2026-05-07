import logging
from pyspark.sql import SparkSession
from src.services.envs import EnvConfig


logger = logging.getLogger(__name__)

def bootstrap_schemas(spark: SparkSession, env: EnvConfig) -> None:

    spark.sql(f"USE CATALOG {env.catalog}")

    schemas = [
        f"{env.schemas['bronze']}",
        f"{env.schemas['silver']}",
        f"{env.schemas['gold']}",
        f"{env.schemas['ops']}",
        f"{env.schemas['smoke_tests']}",
    ]

    for s in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env.catalog}.{s}")
        logger.info("Ensure schema exists: %s", f"{env.catalog}.{s}")