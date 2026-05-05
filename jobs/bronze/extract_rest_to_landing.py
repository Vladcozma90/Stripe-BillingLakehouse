from __future__ import annotations
import os
import argparse
import logging
from datetime import date
from typing import Any
from src.services.envs import load_envs
from src.services.logger import setup_log
from src.connectors.rest import extract_stripe_list_to_landing, StripeCursorSpec
from src.services.secrets import get_kv_secret

logger = logging.getLogger(__name__)

def _get_job_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    return parser.parse_args()


def _get_stripe_cfg(api_sources: dict[str, Any], dataset: str) -> dict[str, Any]:
    stripe = api_sources["stripe"]
    base_url = stripe["base_url"]
    datasets = stripe["datasets"]

    if dataset not in datasets:
        raise ValueError(
            f"Unknown Stripe REST dataset '{dataset}'. "
            f"Available: {list(datasets.keys())}"
        )
    
    endpoint = datasets[dataset]["endpoint"]

    return {
        "base_url": base_url,
        "endpoint": endpoint,
    }



def run_extract_rest_to_landing() -> None:
    args = _get_job_args()
    pipeline_name = f"API_extract_{args.dataset}"
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    setup_log(log_level)
    env = load_envs()

    logger.info("Job start | pipeline_name=%s", pipeline_name)

    try:
        token = get_kv_secret(
            secret_name=env.api_sources["stripe"]["secret_name"],
            key_vault_url=env.azure["key_vault_url"]
        )

        cfg = _get_stripe_cfg(env.api_sources, args.dataset)
        base_url = cfg["base_url"]
        endpoint = cfg["endpoint"]

        extract_date = os.getenv("EXTRACT_DATE", date.today().isoformat())

        landing_dir = os.path.join(
            env.landing_base_path,
            env.project,
            args.dataset,
            f"extract_date={extract_date}",
        )

        page_size = int(os.getenv("PAGE_SIZE", str(StripeCursorSpec().page_size)))

        logger.info("Starting Stripe extract | ENV=%s | dataset=%s | endpoint=%s | landing_dir=%s",
                    os.getenv("ENV", "dev"),
                    args.dataset,
                    endpoint,
                    landing_dir
                    )
    
        manifest = extract_stripe_list_to_landing(
            base_url=base_url,
            endpoint=endpoint,
            landing_dir=landing_dir,
            token=token,
            spec=StripeCursorSpec(page_size=page_size)
        )

        logger.info(
            "Stripe extract finished | dataset=%s | records=%s | pages=%s",
            args.dataset,
            manifest.get("records"),
            manifest.get("pages"),
        )

    except Exception:
        logger.exception("Job failed | pipeline_name=%s", pipeline_name)
        raise

if __name__ == "__main__":
    run_extract_rest_to_landing()


    