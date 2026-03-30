from __future__ import annotations
import os
import argparse
import logging
from datetime import date
from typing import Any
from src.services.envs import load_envs
from src.services.logger import setup_log
from src.connectors.rest import extract_stripe_list_to_landing, StripeCursorSpec

logger = logging.getLogger(__name__)

def _get_job_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    return parser.parse_args()


def _get_stripe_cfg(api_sources: dict[str, Any], dataset: str) -> dict[str, Any]:
    stripe = api_sources["stripe"]
    base_url = stripe["base_url"]
    datasets = stripe["datasets"]
    endpoint = datasets[dataset]["endpoint"]

    if dataset not in datasets:
        raise ValueError(f"Unknown Stripe REST dataset '{dataset}'. Available: {list(datasets.keys())}")
    
    return {"base_url": base_url, "endpoint": endpoint}

def main() -> None:
    env = load_envs()
    setup_log(os.getenv("LOG_LEVEL", "INFO").upper())
    args = _get_job_args()

    dataset = args.dataset
    token = dbutils.secrets.get(scope="kv-prod", key="stripe-api-token")

    cfg = _get_stripe_cfg(env.api_sources, dataset)
    base_url = cfg["base_url"]
    endpoint = cfg["endpoint"]

    extract_date = os.getenv("EXTRACT_DATE", date.today().isoformat())

    landing_dir = os.path.join(
        env.landing_base_path,
        env.project,
        dataset,
        f"extract_date={extract_date}",
    )

    page_size = int(os.getenv("PAGE_SIZE", str(StripeCursorSpec().page_size)))

    logger.info("Starting Stripe extract | ENV=%s | dataset=%s | endpoint=%s | landing_dir=%s",
                os.getenv("ENV", "dev"),
                dataset,
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
        dataset,
        manifest.get("records"),
        manifest.get("pages"),
    )

    if __name__ == "__main__":
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except Exception:
            pass

        main()