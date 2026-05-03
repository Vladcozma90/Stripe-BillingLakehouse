from __future__ import annotations
from typing import Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    count,
    lit,
    when,
    sum as spark_sum,
    countDistinct,
    to_date,
    to_timestamp,
    trim,
    current_timestamp,
    concat_ws
)

def evaluate_dq_rules(
        df: DataFrame,
        rules: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """
    Evaluate batch-level DQ rules against a DataFrame.

    Supported rule types:
    - max_null
    - min_value
    - accepted_values
    - unique
    - valid_date
    - valid_timestamp
    
    returns normalized metrics structure for downstream logging and fail logic.
    """

    if not rules:
        return {
            "total_rows": 0,
            "overall_result": "OK",
            "rule_results": [],
        }
    
    missing_columns = [column_name for column_name in rules if column_name not in df.columns]

    if missing_columns:
        raise ValueError(f"Columns missing from DataFrame for DQ evaluation: {missing_columns}")
    
    agg_exprs = [count(lit(1)).alias("total_rows")]

    for column_name, column_rules in rules.items():
        
        if "max_null" in column_rules:
            agg_exprs.append(
                spark_sum(when(col(column_name).isNull(), 1).otherwise(0)
                .alias(f"{column_name}__null_count"))
            )
        
        if "min_value" in column_rules:
            min_value = column_rules["min_value"]
            agg_exprs.append(
                spark_sum(when(col(column_name).isNotNull() & (col(column_name) < lit(min_value)), 1).otherwise(0))
                .alias(f"{column_name}__min_value_fail_count")
            )

        if "accepted_values" in column_rules:
            accepted_values = column_rules["accepted_values"]
            agg_exprs.append(
                spark_sum(when(col(column_name).isNotNull() & (~col(column_name).isin(*accepted_values)), 1).otherwise(0))
                .alias(f"{column_name}__accepted_values_fail_count")
            )

        if "unique" in column_rules and column_rules["unique"]:
            agg_exprs.append(
                countDistinct(col(column_name)).alias(f"{column_name}__distinct_count")
            )
            agg_exprs.append(
                spark_sum(when(col(column_name).isNull(), 1).otherwise(0))
                .alias(f"{column_name}__null_count_for_unique")
            )

        if "valid_date" in column_rules and column_rules["valid_date"]:
            agg_exprs.append(
                spark_sum(when(col(column_name).isNotNull() & to_date(col(column_name)).isNull(), 1).otherwise(0))
                .alias(f"{column_name}_valid_date_fail_count")
            )

        if "valid_timestamp" in column_rules and column_rules["valid_timestamp"]:
            agg_exprs.append(
                spark_sum(when(col(column_name).isNotNull() & to_timestamp(col(column_name)).isNull(), 1).otherwise(0))
                .alias(f"{column_name}__valid_timestamp_fail_count")
            )
        
        agg_row = df.agg(*agg_exprs).collect()[0]
        total_rows = agg_row["total_rows"]

        rule_results: list[dict[str, Any]] = []
        overall_result = "OK"

        for column_name, column_rules in rules.items():
            severity = column_rules.get("severity", "error").upper()
            severity = severity.upper()

            if "max_null" in column_rules:
                null_count = int(agg_row[f"{column_name}__null_count"])
                actual_value = (null_count / total_rows) if total_rows else 0.0
                threshold_value = float(column_rules["max_null"])
                dq_result = "FAIL" if actual_value > threshold_value else "OK"

                if dq_result == "FAIL" and severity == "ERROR":
                    overall_result = "FAIL"
                
                rule_results.append(
                    {
                        "column_name": column_name,
                        "rule_name": "max_null",
                        "actual_value": float(actual_value),
                        "threshold_value": threshold_value,
                        "failed_rows": null_count,
                        "severity": severity,
                        "dq_result": dq_result,
                    }
                )

            if "min_value" in column_rules:
                fail_count = int(agg_row[f"{column_name}__min_value_fail_count"])
                actual_value = (fail_count / total_rows) if total_rows else 0.0
                threshold_value = float(column_rules["min_value"])
                dq_result = "FAIL" if actual_value > threshold_value else "OK"

                if dq_result == "FAIL" and severity == "ERROR":
                    overall_result = "FAIL"
                
                rule_results.append(
                    {
                        "column_name": column_name,
                        "rule_name": "min_value",
                        "actual_value": float(actual_value),
                        "threshold_value": threshold_value,
                        "failed_rows": fail_count,
                        "severity": severity,
                        "dq_result": dq_result,
                    }
                )
            
            if "accepted_values" in column_rules:
                fail_count = int(agg_row[f"{column_name}__accepted_values_fail_count"])
                actual_value = (fail_count / total_rows) if total_rows else 0.0
                threshold_value = float(column_rules["accepted_values"])
                dq_result = "FAIL" if actual_value > threshold_value else "OK"

                if dq_result == "FAIL" and severity == "ERROR":
                    overall_result = "FAIL"
                
                rule_results.append(
                    {
                        "column_name": column_name,
                        "rule_name": "accepted_values",
                        "acutal_value": float(actual_value),
                        "threshold_value": threshold_value,
                        "failed_rows": fail_count,
                        "severity": severity,
                        "dq_result": dq_result,
                    }
                )
            
            if "unique" in column_rules and column_rules["unique"]:
                distinct_count = int(agg_row[f"{column_name}__distinct_count"])
                null_count_for_unique = int(agg_row[f"{column_name}__null_count_for_unique"])
                non_null_rows = total_rows - null_count_for_unique
                duplicate_count = max(non_null_rows - distinct_count, 0)
                actual_value = int(duplicate_count / total_rows) if total_rows else 0.0
                threshold_value = float(column_rules["unique"])
                dq_result = "FAIL" if actual_value > threshold_value else "OK"

                if dq_result == "FAIL" and severity == "ERROR":
                    overall_result = "FAIL"
                
                rule_results.append(
                    {
                        "column_name": column_name,
                        "rule_name": "unique",
                        "actual_value": float(actual_value),
                        "threshold_value": threshold_value,
                        "failed_rows": duplicate_count,
                        "severity": severity,
                        "dq_result": dq_result,
                    }
                )

            if "valid_date" in column_rules and column_rules["valid_date"]:
                fail_count = int(agg_row[f"{column_name}__valid_date_fail_count"])
                actual_value = (fail_count / total_rows) if total_rows else 0.0
                threshold_value = 0.0
                dq_result = "FAIL" if fail_count > 0 else "OK"

                if dq_result == "FAIL" and severity == "ERROR":
                    overall_result = "FAIL"

                rule_results.append(
                    {
                        "column_name": column_name,
                        "rule_name": "valid_date",
                        "actual_value": float(actual_value),
                        "threshold_value": threshold_value,
                        "failed_rows": fail_count,
                        "severity": severity,
                        "dq_result": dq_result,
                    }
                )

            if "valid_timestamp" in column_rules and column_rules["valid_timestamp"]:
                fail_count = int(agg_row[f"{column_name}__valid_timestamp_fail_count"])
                actual_value = int(fail_count / total_rows) if total_rows else 0.0
                threshold_value = 0.0
                dq_result = "FAIL" if actual_value > threshold_value else "OK"

                if dq_result == "FAIL" and severity == "ERROR":
                    overall_result = "FAIL"

                rule_results.append(
                    {
                        "column_name": column_name,
                        "rule_name": "valid_timestamp",
                        "acutal_value": float(actual_value),
                        "threshold_value": threshold_value,
                        "failed_rows": fail_count,
                        "severity": severity,
                        "dq_result": dq_result
                    }
                )

        return {
            "total_rows": total_rows,
            "rule_results": rule_results,
            "overall_result": overall_result
        }
    

def build_dq_results_df(
        spark: SparkSession,
        dq_source: str,
        run_id: str,
        metrics: dict[str, Any],
) -> DataFrame:
    
    rows = [(
        dq_source,
        run_id,
        datetime.utcnow(),
        int(metrics["total_rows"]),
        rule_result["column_name"],
        rule_result["rule_name"],
        float(rule_result["acutal_value"]),
        float(rule_result["threshold_value"]),
        int(rule_result["failed_rows"]),  
        rule_result["severity"],
        rule_result["dq_result"],
        )
        for rule_result in metrics["rule_results"]
    ]

    return spark.createDataFrame(
        rows,
        """
        dq_source_table STRING,
        run_id STRING,
        dq_ts TIMESTAMP,
        total_rows BIGINT,
        column_name STRING,
        rule_name STRING,
        acutal_value DOUBLE,
        threshold_value DOUBLE,
        failed_rows BIGINT,
        severity STRING,
        dq_result STRING
        """,
    )


def build_dq_failure_message(metrics: dict[str, Any]) -> str:

    failed_rules = [r for r in metrics["rule_results"] if r["dq_result"] == "FAIL"]

    if not failed_rules:
        return "DQ FAIL"
    
    parts = [
        f"{r["column_name"]}:{r["rule_name"]} actual={r["actual_value"]:.4f}"
        f"threshold={r["threshold_value"]:.4f} failed_rows={r["failed_rows"]:.4f} severity={r["severity"]}"
        for r in failed_rules
    ]

    return "DQ FAIL | " + " | ".join(parts)





def quarantine_by_business_key(
        stage_df: DataFrame,
        key_columns: list[str]
) -> tuple[DataFrame, DataFrame]:
    
    if not key_columns:
        raise ValueError("key_columns must not be empty.")
    
    missing_columns = [c for c in key_columns if c not in stage_df.columns]
    if missing_columns:
        raise ValueError(f"Business key columns missing from stage_df: {missing_columns}")
    
    invalid_conditions = [
        col(c).isNull() | (trim(col(c).cast("string")) == "")
        for c in key_columns
    ]

    invalid_key_condition = invalid_conditions[0]
    for condition in invalid_conditions[1:]:
        invalid_key_condition = invalid_key_condition | condition

    quarantine_reason = concat_ws(
        ";",
        *[
            when(
                col(c).isNull() | (trim(col(c).cast("string")) == ""),
                lit(f"{c} is NULL/BLANK")
            )
            for c in key_columns
        ]
    )

    bad_records = (
        stage_df
        .filter(invalid_key_condition)
        .withColumn("quarantine_reason", quarantine_reason)
        .withColumn("quarantine_ts", current_timestamp())
    )
    
    good_records = stage_df.filter(~invalid_key_condition)

    return bad_records, good_records