from __future__ import annotations

import importlib


JOB_MODULES = [
    # Bronze / extraction
    "jobs.bronze.extract_rest_to_landing",
    "jobs.bronze.run_bronze",

    # Bootstrap
    "jobs.bootstrap.run_bootstrap",

    # Silver
    "jobs.silver.run_silver_erp_account_master_snapshot",
    "jobs.silver.run_silver_erp_plan_catalog",
    "jobs.silver.run_silver_erp_usage_daily",
    "jobs.silver.run_silver_stripe_customers",
    "jobs.silver.run_silver_stripe_invoices",
    "jobs.silver.run_silver_stripe_subscription_items",
    "jobs.silver.run_silver_stripe_subscriptions",

    # Gold
    "jobs.gold.run_gold_fact_billing_revenue",

    # Databricks smoke tests
    "tests.smoke_test.run_bootstrap_test",
    "tests.smoke_test.run_e2e_smoke_test",
]


def test_job_modules_import_successfully() -> None:
    failed_imports: list[tuple[str, str]] = []

    for module_name in JOB_MODULES:
        try:
            importlib.import_module(module_name)
        except Exception as exc:
            failed_imports.append((module_name, repr(exc)))

    assert not failed_imports, (
        "Some job modules failed to import:\n"
        + "\n".join(
            f"- {module_name}: {error}"
            for module_name, error in failed_imports
        )
    )