from __future__ import annotations
import os
import logging
import argparse
from pyspark.sql import SparkSession

from src.services.envs import load_envs
from src.services.logger import setup_log
from pipelines.bronze.bronze import ingest_bronze


logger = logging.getLogger(__name__)

def _get_job_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    return parser.parse_args()

def bronze_job_run() -> None:
    args = _get_job_args()
    env = load_envs()
    setup_log(os.getenv("LOG_LEVEL", "INFO").upper())

    spark = SparkSession.builder.appName(f"bronze_ingest_{args.dataset}").getOrCreate()

    try:
        ingest_bronze(spark, env, args.dataset)
    finally:
        spark.stop()

if __name__ == '__main__':
    bronze_job_run()

