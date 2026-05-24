from __future__ import annotations

import argparse
import logging
import os
from datetime import date
from typing import Any

from pyspark.sql import SparkSession

from src.connectors.rest import StripeCursorSpec, extract_stripe_list_to_landing
from src.services.envs import load_envs
from src.services.logger import setup_log
from src.services.secrets import get_kv_secret

logger = logging.getLogger(__name__)


def _get_job_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--dataset", required=True)

    parser.add_argument(
        "--extract-date",
        default=date.today().isoformat(),
        help="Logical extract date used in the landing partition path. Format: YYYY-MM-DD.",
    )

    parser.add_argument(
        "--page-size",
        type=int,
        default=StripeCursorSpec().page_size,
        help="Stripe page size for cursor pagination.",
    )

    parser.add_argument(
        "--timeout-s",
        type=int,
        default=60,
        help="HTTP request timeout in seconds.",
    )

    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum number of retries for retryable Stripe API failures.",
    )

    parser.add_argument(
        "--max-backoff-s",
        type=int,
        default=60,
        help="Maximum exponential backoff sleep time in seconds.",
    )

    return parser.parse_args()


def _get_stripe_cfg(api_sources: dict[str, Any], dataset: str) -> dict[str, Any]:
    stripe = api_sources["stripe"]
    base_url = stripe["base_url"]
    datasets = stripe["datasets"]

    if dataset not in datasets:
        raise ValueError(
            f"Unknown Stripe REST dataset '{dataset}'. "
            f"Available datasets: {list(datasets.keys())}"
        )

    endpoint = datasets[dataset]["endpoint"]

    return {
        "base_url": base_url,
        "endpoint": endpoint,
        "secret_name": stripe["secret_name"],
    }


def _validate_args(args: argparse.Namespace) -> None:
    if args.page_size <= 0:
        raise ValueError(f"--page-size must be greater than 0. Got: {args.page_size}")

    if args.timeout_s <= 0:
        raise ValueError(f"--timeout-s must be greater than 0. Got: {args.timeout_s}")

    if args.max_retries < 0:
        raise ValueError(f"--max-retries cannot be negative. Got: {args.max_retries}")

    if args.max_backoff_s <= 0:
        raise ValueError(
            f"--max-backoff-s must be greater than 0. Got: {args.max_backoff_s}"
        )


def run_extract_rest_to_landing() -> None:
    args = _get_job_args()
    _validate_args(args)

    pipeline_name = f"api_extract_{args.dataset}"
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    setup_log(log_level)
    env = load_envs()

    spark = SparkSession.builder.appName(pipeline_name).getOrCreate()

    logger.info(
        "Job start | pipeline_name=%s | dataset=%s | extract_date=%s",
        pipeline_name,
        args.dataset,
        args.extract_date,
    )

    try:
        cfg = _get_stripe_cfg(env.api_sources, args.dataset)

        token = get_kv_secret(
            secret_name=cfg["secret_name"],
            key_vault_url=env.azure["key_vault_url"],
        )

        landing_dir = (
            f"{env.landing_base_path}/"
            f"{env.catalog}/landing_data/{args.dataset}/"
            f"extract_date={args.extract_date}"
        )

        logger.info(
            "Starting Stripe extract | env=%s | dataset=%s | endpoint=%s | landing_dir=%s | page_size=%s",
            os.getenv("ENV", "dev"),
            args.dataset,
            cfg["endpoint"],
            landing_dir,
            args.page_size,
        )

        manifest = extract_stripe_list_to_landing(
            spark=spark,
            base_url=cfg["base_url"],
            endpoint=cfg["endpoint"],
            landing_dir=landing_dir,
            token=token,
            spec=StripeCursorSpec(page_size=args.page_size),
            timeout_s=args.timeout_s,
            max_retries=args.max_retries,
            max_backoff_s=args.max_backoff_s,
        )

        logger.info(
            "Stripe extract finished | dataset=%s | records=%s | pages=%s | landing_path=%s",
            args.dataset,
            manifest.get("records"),
            manifest.get("pages"),
            manifest.get("landing_path"),
        )

    except Exception:
        logger.exception(
            "Job failed | pipeline_name=%s | dataset=%s | extract_date=%s",
            pipeline_name,
            args.dataset,
            args.extract_date,
        )
        raise


if __name__ == "__main__":
    run_extract_rest_to_landing()