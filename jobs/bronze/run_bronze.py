from __future__ import annotations
import os
import logging
import argparse
from pyspark.sql import SparkSession

from src.services.envs import load_envs
from src.services.logger import setup_log
from src.pipelines.bronze.bronze_ingestion import ingest_bronze


logger = logging.getLogger(__name__)

def _get_job_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    return parser.parse_args()

def job_run_bronze() -> None:
    args = _get_job_args()
    pipeline_name = f"bronze_ingest_{args.dataset}"
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    setup_log(log_level)
    env = load_envs()

    logger.info("Job start | pipeline_name=%s | env=%s", pipeline_name, os.getenv("ENV", "dev"))

    spark = SparkSession.builder.appName(pipeline_name).getOrCreate()

    try:
        ingest_bronze(spark, env, args.dataset)
        logger.info("Job success | pipeline_name=%s", pipeline_name)

    except Exception:
        logger.exception("Job failed | pipeline_name=%s", pipeline_name)
        raise

    finally:
        spark.stop()
        logger.info("spark session stopped | pipeline_name=%s", pipeline_name)

if __name__ == '__main__':
    job_run_bronze()