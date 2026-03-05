from typing import Any, Iterable
from pyspark.sql import SparkSession
from src.utils.envs import load_envs, EnvConfig
from src.utils.logger import setup_log
import logging
import os

logger = logging.getLogger(__name__)

def _ensure_schemas(spark: SparkSession, env: EnvConfig, schemas: Iterable[str]) -> None:

    spark.sql(f"USE CATALOG {env.catalog}")
    
    for s in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env.catalog}.{s}")
        logger.info("Ensure schema exists: %s", f"{env.catalog}.{s}")

def bootstrap(spark: SparkSession, env: EnvConfig) -> dict[str, Any]:

    schemas = [
        f"{env.project}_ops",
        f"{env.project}_bronze",
        f"{env.project}_silver",
        f"{env.project}_gold",
    ]
    _ensure_schemas(spark, env, schemas)

    ops_schema = f"{env.catalog}.{env.project}_ops"

    ops = {
        "run_logs_table" : f"{ops_schema}.run_logs",
        "run_logs_path" : f"{env.ops_base_path}/{env.project}_ops/run_logs",

        "state_table" : f"{ops_schema}.pipeline_state",
        "state_path" : f"{env.ops_base_path}/{env.project}_pipeline_state",
    }
    logger.info("Creating/validating OPS tables in schema %s", ops_schema)

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {ops["run_logs_table"]} (
                pipeline_name STRING,
                dataset STRING,
                target_table STRING,
                run_id STRING,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                status STRING,
                rows_in BIGINT,
                rows_quarantined BIGINT,
                rows_out BIGINT,
                error_msg STRING,
                dq_result STRING,
                last_watermark_ts TIMESTAMP
                )
            USING DELTA
            LOCATION '{ops["run_logs_path"]}'
            """)
    
    logger.info("Ensure table exists: %s", f"{ops["run_logs_table"]}")


    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {ops["state_table"]} (
                pipeline_name STRING,
                dataset STRING,
                last_watermark_ts TIMESTAMP,
                updated_by_run_id STRING,
                updated_at TIMESTAMP
                )
            USING DELTA
            LOCATION '{ops["state_path"]}'
            """)
    
    logger.info("Ensure table exists: %s", f"{ops["state_table"]}")

    for t in (ops["run_logs_table"], ops["state_table"]):
        spark.sql(f"""
                    ALTER TABLE {t}
                    SET TBLPROPRIETIES(
                    delta.autoOptimize.optimizeWrite = 'true',
                    delta.autoOptimize.autoCompact = 'true'
                    )
                """)
        logger.info("Applied Delta optimize proprieties: %s", t)

    logger.info("Bootstrap complete.")
    return ops

def main() -> None:
    env = load_envs()
    setup_log(os.getenv("LOG_LEVEL", "INFO").upper())
    spark = SparkSession.builder.appName("booststrap").getOrCreate()

    bootstrap(spark, env)

    if __name__ == "__main__":
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except Exception:
            pass
        main()