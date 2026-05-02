from __future__ import annotations

import os
import logging
from collections.abc import Callable

from pyspark.sql import SparkSession

from src.services.envs import load_envs
from src.services.logger import setup_log

from bootstrap.bootstrap_schemas import bootstrap_schemas
from bootstrap.bootstrap_ops import bootstrap_ops
from bootstrap.bootstrap_erp_account_master_snapshot import bootstrap_erp_account_master_snapshot
from bootstrap.bootstrap_erp_plan_catalog import bootstrap_erp_plan_catalog
from bootstrap.bootstrap_erp_usage_daily import bootstrap_erp_usage_daily
from bootstrap.bootstrap_stripe_customers import bootstrap_stripe_customers
from bootstrap.bootstrap_stripe_invoices import bootstrap_stripe_invoices
from bootstrap.bootstrap_stripe_subscription_items import bootstrap_subscription_items
from bootstrap.bootstrap_stripe_subscriptions import bootstrap_stripe_subscriptions


logger = logging.getLogger(__name__)


def job_run_bootstrap() -> None:
    pipeline_name = "bootstrap"
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    setup_log(log_level)
    env = load_envs()

    logger.info(
        "Job start | pipeline_name=%s | env=%s",
        pipeline_name,
        os.getenv("ENV", "dev"),
    )

    spark = SparkSession.builder.appName(pipeline_name).getOrCreate()

    bootstrap_files: list[tuple[str, Callable]] = [
        ("bootstrap_schemas", bootstrap_schemas),
        ("bootstrap_ops", bootstrap_ops),
        ("bootstrap_erp_account_master_snapshot", bootstrap_erp_account_master_snapshot),
        ("bootstrap_erp_plan_catalog", bootstrap_erp_plan_catalog),
        ("bootstrap_erp_usage_daily", bootstrap_erp_usage_daily),
        ("bootstrap_stripe_customers", bootstrap_stripe_customers),
        ("bootstrap_stripe_invoices", bootstrap_stripe_invoices),
        ("bootstrap_stripe_subscription_items", bootstrap_subscription_items),
        ("bootstrap_stripe_subscriptions", bootstrap_stripe_subscriptions),
    ]

    try:
        for file_name, file_func in bootstrap_files:
            logger.info("Bootstrap step start | step=%s", file_name)
            file_func(spark=spark, env=env)
            logger.info("Bootstrap step success | step=%s", file_name)

        logger.info("Job success | pipeline_name=%s", pipeline_name)

    except Exception:
        logger.exception("Job failed | pipeline_name=%s", pipeline_name)
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped | pipeline_name=%s", pipeline_name)

if __name__ == "__main__":
    job_run_bootstrap()