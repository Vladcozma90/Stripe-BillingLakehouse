import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import yaml


@dataclass(frozen=True)
class EnvConfig:
    catalog: str
    project: str
    landing_base_path: str
    bronze_base_path: str
    silver_base_path: str
    gold_base_path: str
    ops_base_path: str
    checkpoint_base_path: str
    api_sources: dict[str, Any]
    datasets: dict[str, Any]
    key_vault_url: str


def _require_str(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Expected {name} to be a non-empty string, got {type(value).__name__}")
    return value.strip()

def _require_list_str(value: Any, name: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(i, str) and i.strip() for i in value)
    ):
        raise ValueError(f"Expected {name} to be a non-empty list of non-empty strings, got {type(value).__name__}")
    return [i.strip() for i in value]


def _require_dict(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected {name} to be a dict, got {type(value).__name__}")
    return value


def _validate_api_sources(api_sources: dict[str, Any]) -> None:
    stripe = api_sources.get("stripe")
    if not isinstance(stripe, dict):
        raise ValueError("api_sources.stripe is missing or not a dict")

    base_url = stripe.get("base_url")
    if not isinstance(base_url, str) or not base_url.strip():
        raise ValueError("api_sources.stripe.base_url is missing or empty")

    datasets = stripe.get("datasets")
    if not isinstance(datasets, dict) or not datasets:
        raise ValueError("api_sources.stripe.datasets is missing or empty")

    for ds_name, ds_cfg in datasets.items():
        if not isinstance(ds_cfg, dict):
            raise ValueError(f"api_sources.stripe.datasets.{ds_name} must be a dict")
        endpoint = ds_cfg.get("endpoint")
        if not isinstance(endpoint, str) or not endpoint.strip():
            raise ValueError(f"api_sources.stripe.datasets.{ds_name}.endpoint must be a non-empty string")


def load_envs() -> EnvConfig:
    env = os.getenv("ENV", "dev").strip().lower()

    cfg_base = Path(os.getenv("APP_CONFIG_DIR", "/opt/airflow/app/configs"))
    cfg_path = cfg_base / f"{env}.yaml"

    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as f:
        cfg: dict[str, Any] = yaml.safe_load(f) or {}



    paths = _require_dict(cfg.get("paths", {}), "paths")
    api_sources = _require_dict(cfg.get("api_sources", {}), "api_sources")
    _validate_api_sources(api_sources)


    return EnvConfig(
        catalog=_require_str(cfg.get("catalog"), "catalog"),
        project=_require_str(cfg.get("project"), "project"),
        landing_base_path=_require_str(paths.get("landing_base_path"), "paths.landing_base_path"),
        bronze_base_path=_require_str(paths.get("bronze_base_path"), "paths.bronze_base_path"),
        silver_base_path=_require_str(paths.get("silver_base_path"), "paths.silver_base_path"),
        gold_base_path=_require_str(paths.get("gold_base_path"), "paths.gold_base_path"),
        ops_base_path=_require_str(paths.get("ops_base_path"), "ops_base_path"),
        checkpoint_base_path=_require_str(paths.get("checkpoint_base_path"), "paths.checkpoint_base_path"),
        api_sources=api_sources,
        datasets=_require_dict(cfg.get("datasets", {}), "datasets"),
        key_vault_url=_require_str(cfg.get("key_vault_url"), "key_vault_url"),
    )