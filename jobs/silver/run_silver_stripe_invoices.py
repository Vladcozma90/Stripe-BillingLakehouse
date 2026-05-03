from __future__ import annotations
import os
import logging
from pyspark.sql import SparkSession
from src.pipelines.silver.silver_stripe_inovices import run_silver_stripe_invoices
from src.services.logger import setup_log
from src.services.envs import load_envs


logger = logging.getLogger(__name__)

def job_run_silver_stripe_invoices() -> None:
    pipeline_name = "silver_stripe_invoices"
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    setup_log(log_level)
    env = load_envs()

    logger.info("Job start | pipeline_name=%s | env=%s", pipeline_name, os.getenv("ENV", "dev"))

    spark = SparkSession.builder.appName(pipeline_name).getOrCreate()

    try:
        run_silver_stripe_invoices(spark=spark, env=env)
        logger.info("Job success | pipeline_name=%s", pipeline_name)
    
    except Exception:
        logger.info("Job failed | pipeline_name=%s", pipeline_name)
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped | pipeline_name=%s", pipeline_name)

if __name__ == '__main__':
    job_run_silver_stripe_invoices()
