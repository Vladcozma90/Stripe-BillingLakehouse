from __future__ import annotations
import logging
import os
from pyspark.sql import SparkSession
from src.pipelines.gold.gold_fact_stripe_invoices import run_gold_fact_invoices
from src.services.envs import load_envs
from src.services.logger import setup_log


logger = logging.getLogger(__name__)

def job_run_gold_fact_stripe_invoices() -> None:
    pipeline_name = "gold_fact_stripe_invoices"
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    setup_log(log_level)
    env = load_envs()

    logger.info("Job start | pipeline_name=%s | env=%s", pipeline_name, os.getenv("ENV", "dev"))

    spark = SparkSession.builder.appName(pipeline_name).getOrCreate()

    try:
        run_gold_fact_invoices(spark=spark, env=env)
        logger.info("Job success | pipeline_name=%s", pipeline_name)

    except Exception:
        logger.exception("Job failed | pipeline_name=%s", pipeline_name)
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped | pipeline_name=%s", pipeline_name)

if __name__ == '__main__':
    job_run_gold_fact_stripe_invoices()
    
