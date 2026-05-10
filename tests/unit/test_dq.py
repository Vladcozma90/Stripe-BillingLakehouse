from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pyspark.sql import SparkSession

from src.services.dq import evaluate_dq_rules, build_dq_failure_message


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("billinglakehouse-dq-unit-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def dev_config() -> dict:
    config_path = Path(__file__).resolve().parents[2] / "configs" / "dev.yaml"

    with config_path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def test_stripe_invoices_dq_detects_invalid_records(
    spark: SparkSession,
    dev_config: dict,
) -> None:
    rules = dev_config["datasets"]["stripe_invoices"]["data_quality"]["rules"]

    df = spark.createDataFrame(
        [
            (
                "inv_001",
                "cus_001",
                "paid",
                "charge_automatically",
                "USD",
                100.0,
                100.0,
                0.0,
                100.0,
                100.0,
                1,
                "2026-01-01 10:00:00",
                None,
                "sub_001",
                "pi_001",
            ),
            (
                "inv_002",
                "cus_002",
                "invalid_status",
                "charge_automatically",
                "USD",
                -10.0,
                0.0,
                0.0,
                -10.0,
                -10.0,
                1,
                "bad_timestamp",
                None,
                "sub_002",
                "pi_002",
            ),
        ],
        [
            "invoice_id",
            "stripe_customer_id",
            "invoice_status",
            "collection_method",
            "currency",
            "amount_due",
            "amount_paid",
            "amount_remaining",
            "subtotal",
            "total",
            "attempt_count",
            "created_ts",
            "due_date_ts",
            "subscription_id",
            "payment_intent_id",
        ],
    )

    metrics = evaluate_dq_rules(
        df=df,
        rules=rules,
        dataset="stripe_invoices",
    )

    assert metrics["overall_result"] == "FAIL"

    failed_rules = [
        (rule["column_name"], rule["rule_name"])
        for rule in metrics["rule_results"]
        if rule["dq_result"] == "FAIL"
    ]

    assert ("invoice_status", "accepted_values") in failed_rules
    assert ("amount_due", "min_value") in failed_rules
    assert ("subtotal", "min_value") in failed_rules
    assert ("total", "min_value") in failed_rules
    assert ("created_ts", "valid_timestamp") in failed_rules


def test_stripe_invoices_dq_passes_valid_records(
    spark: SparkSession,
    dev_config: dict,
) -> None:
    rules = dev_config["datasets"]["stripe_invoices"]["data_quality"]["rules"]

    df = spark.createDataFrame(
        [
            (
                "inv_001",
                "cus_001",
                "paid",
                "charge_automatically",
                "USD",
                100.0,
                100.0,
                0.0,
                100.0,
                100.0,
                1,
                "2026-01-01 10:00:00",
                None,
                "sub_001",
                "pi_001",
            ),
            (
                "inv_002",
                "cus_002",
                "open",
                "send_invoice",
                "EUR",
                50.0,
                0.0,
                50.0,
                50.0,
                50.0,
                0,
                "2026-01-02 11:00:00",
                None,
                "sub_002",
                "pi_002",
            ),
        ],
        [
            "invoice_id",
            "stripe_customer_id",
            "invoice_status",
            "collection_method",
            "currency",
            "amount_due",
            "amount_paid",
            "amount_remaining",
            "subtotal",
            "total",
            "attempt_count",
            "created_ts",
            "due_date_ts",
            "subscription_id",
            "payment_intent_id",
        ],
    )

    metrics = evaluate_dq_rules(
        df=df,
        rules=rules,
        dataset="stripe_invoices",
    )

    assert metrics["overall_result"] == "OK"


def test_erp_plan_catalog_dq_detects_duplicate_and_invalid_values(
    spark: SparkSession,
    dev_config: dict,
) -> None:
    rules = dev_config["datasets"]["erp_plan_catalog"]["data_quality"]["rules"]

    df = spark.createDataFrame(
        [
            (
                "BASIC",
                "Basic Plan",
                10.0,
                5,
                1000,
                "USD",
                "monthly",
                "2026-01-01",
                None,
                True,
                1,
            ),
            (
                "BASIC",
                "Basic Plan Duplicate",
                -5.0,
                0,
                -10,
                "INVALID",
                "weekly",
                "bad_date",
                None,
                True,
                1,
            ),
        ],
        [
            "plan_code",
            "plan_name",
            "monthly_price_usd",
            "seats_included",
            "max_units_per_month",
            "currency",
            "billing_period",
            "effective_from",
            "effective_to",
            "is_current",
            "price_version",
        ],
    )

    metrics = evaluate_dq_rules(
        df=df,
        rules=rules,
        dataset="erp_plan_catalog",
    )

    assert metrics["overall_result"] == "FAIL"

    failed_rules = [
        (rule["column_name"], rule["rule_name"])
        for rule in metrics["rule_results"]
        if rule["dq_result"] == "FAIL"
    ]

    assert ("monthly_price_usd", "min_value") in failed_rules
    assert ("seats_included", "min_value") in failed_rules
    assert ("max_units_per_month", "min_value") in failed_rules
    assert ("currency", "accepted_values") in failed_rules
    assert ("billing_period", "accepted_values") in failed_rules
    assert ("effective_from", "valid_date") in failed_rules


def test_build_failure_message_contains_failed_rule_details(
    spark: SparkSession,
    dev_config: dict,
) -> None:
    rules = dev_config["datasets"]["stripe_invoices"]["data_quality"]["rules"]

    df = spark.createDataFrame(
        [
            (
                "inv_001",
                "cus_001",
                "invalid_status",
                "charge_automatically",
                "USD",
                -10.0,
                0.0,
                0.0,
                -10.0,
                -10.0,
                1,
                "bad_timestamp",
                None,
                "sub_001",
                "pi_001",
            ),
        ],
        [
            "invoice_id",
            "stripe_customer_id",
            "invoice_status",
            "collection_method",
            "currency",
            "amount_due",
            "amount_paid",
            "amount_remaining",
            "subtotal",
            "total",
            "attempt_count",
            "created_ts",
            "due_date_ts",
            "subscription_id",
            "payment_intent_id",
        ],
    )

    metrics = evaluate_dq_rules(
        df=df,
        rules=rules,
        dataset="stripe_invoices",
    )

    message = build_dq_failure_message(metrics)

    assert "invoice_status" in message
    assert "accepted_values" in message
    assert "amount_due" in message
    assert "min_value" in message