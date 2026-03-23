from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import count, lit, when, col

def evaluate_null_rules(
        df: DataFrame,
        rules: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    
    if not rules:
        return {
            "total_rows" : 0,
            "overall_result" : "OK",
            "rule_results" : [],
        }
    
    missing_columns = [column_name for column_name in rules if column_name not in df.columns]
    if missing_columns:
        raise ValueError(f"Columns missing form DataFrame for DQ evaluation: {missing_columns}")
    
    agg_exprs = [count(lit(1)).alias("total_rows")]

    for column_name, column_rules in rules.items():
        if "max_null" in column_rules:
            agg_exprs.append(
                sum(
                    when(col(column_name).isNull(), 1).otherwise(0)
                ).alias(f"{column_name}__null_count")
            )

    agg_row = df.agg(*agg_exprs).collect()[0]

    total_rows = int(agg_row["total_rows"])
    rule_results: list[dict[str, Any]] = []
    overall_result = "OK"

    for column_name, column_rules in rules.items():
        if "max_null" not in column_rules:
            continue
            
        null_count = int(agg_row[f"{column_name}__null_count"])
        null_pct = (null_count / total_rows) if total_rows else 0.0
        threshold_value = float(column_rules["max_null"])
        
        dq_result = "FAIL" if null_pct > threshold_value else "OK"

        if dq_result == "FAIL":
            overall_result = "FAIL"
        
        rule_results.append(
            {
                "column_name" : column_name,
                "rule_name" : column_rules,
                "actual_value" : float(null_pct),
                "threshold_value" : threshold_value,
                "dq_result" : dq_result
            }
        )
    
    return {
        "total_rows": total_rows,
        "rule_results" : rule_results,
        "overall_result" : overall_result,
    }

def build_dq_results_df(
        spark: SparkSession,
        table_name: str,
        run_id: str,
        dq_scope: str,
        metrics: dict[str, Any],
) -> DataFrame:
    
    rows = [
        (
            table_name,
            run_id,
            datetime.utcnow(),
            dq_scope,
            int(metrics["total_rows"]),
            rule_result["column_name"],
            rule_result["rule_name"],
            float(rule_result["acutal_value"]),
            float(rule_result["threshold_value"]),
            rule_result["dq_result"],
        )
        for rule_result in metrics["rule_results"]
    ]

    return spark.createDataFrame(
        rows,
        """
        table_name STRING,
        run_id STRING,
        dq_ts TIMESTAMP,
        dq_scope STRING,
        total_rows BIGINT,
        column_name STRING,
        rule_name STRING,
        actual_value DOUBLE,
        threshold_value DOUBLE,
        dq_result STRING
        """,
    )

def build_dq_failure_message(metrics: dict[str, Any]) -> str:
    failed_rules = [r for r in metrics["rule_results"] if r["dq_result"] == "FAIL"]

    if not failed_rules:
        return "DQ FAIL"
    
    parts = [
        f"{r["column_name"]}:{r["rule_name"]} actual={r["acutal_value"]:.4f} threshold={r["threshold_value"]:.4f}"
        for r in failed_rules
    ]

    return "DQ FAIL | " + " | ".join(parts)