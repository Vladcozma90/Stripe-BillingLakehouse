from __future__ import annotations

import pytest

from src.services.envs import EnvConfig, load_envs


def test_load_envs_loads_dev_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENV", "dev")

    env = load_envs()

    assert isinstance(env, EnvConfig)

    assert env.catalog
    assert env.project

    assert env.landing_base_path
    assert env.bronze_base_path
    assert env.silver_base_path
    assert env.gold_base_path
    assert env.ops_base_path
    assert env.checkpoint_base_path

    assert isinstance(env.azure, dict)
    assert "key_vault_url" in env.azure
    assert env.azure["key_vault_url"]

    assert isinstance(env.api_sources, dict)
    assert "stripe" in env.api_sources

    assert isinstance(env.datasets, dict)
    assert env.datasets


def test_dev_config_contains_required_stripe_api_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENV", "dev")

    env = load_envs()

    stripe_cfg = env.api_sources["stripe"]

    assert stripe_cfg["base_url"] == "https://api.stripe.com/v1"
    assert stripe_cfg["secret_name"]

    assert "datasets" in stripe_cfg

    expected_datasets = {
        "stripe_customers",
        "stripe_subscriptions",
        "stripe_subscription_items",
        "stripe_invoices",
    }

    assert expected_datasets.issubset(set(stripe_cfg["datasets"].keys()))

    for dataset_name in expected_datasets:
        assert stripe_cfg["datasets"][dataset_name]["endpoint"]


def test_dev_config_contains_required_pipeline_datasets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENV", "dev")

    env = load_envs()

    expected_datasets = {
        "erp_plan_catalog",
        "erp_account_master_snapshot",
        "erp_usage_daily",
        "stripe_customers",
        "stripe_subscriptions",
        "stripe_subscription_items",
        "stripe_invoices",
    }

    assert expected_datasets.issubset(set(env.datasets.keys()))


def test_each_dataset_has_landing_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENV", "dev")

    env = load_envs()

    for dataset_name, dataset_cfg in env.datasets.items():
        assert "landing_format" in dataset_cfg, (
            f"Dataset '{dataset_name}' is missing landing_format"
        )

        assert dataset_cfg["landing_format"] in {"json", "parquet"}, (
            f"Dataset '{dataset_name}' has invalid landing_format: "
            f"{dataset_cfg['landing_format']}"
        )


def test_each_dataset_has_dq_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENV", "dev")

    env = load_envs()

    for dataset_name, dataset_cfg in env.datasets.items():
        assert "data_quality" in dataset_cfg, (
            f"Dataset '{dataset_name}' is missing data_quality section"
        )

        assert "rules" in dataset_cfg["data_quality"], (
            f"Dataset '{dataset_name}' is missing data_quality.rules"
        )

        assert dataset_cfg["data_quality"]["rules"], (
            f"Dataset '{dataset_name}' has empty DQ rules"
        )


def test_invalid_env_raises_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENV", "does_not_exist")

    with pytest.raises(Exception):
        load_envs()