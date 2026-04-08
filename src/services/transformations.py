from pyspark.sql import DataFrame
from pyspark.sql.functions import (col, row_number)
from pyspark.sql.window import Window


def deduplicate_by_business_key(
        df: DataFrame,
        key_columns: list[str],
        order_columns: list[str] | None = None
) -> DataFrame:
    if not key_columns:
        raise ValueError("key_columns must not be empty")
    
    missing_key_columns =  [c for c in key_columns if c not in df.columns]
    if missing_key_columns:
        raise ValueError(f"Business key columns missing from df: {missing_key_columns}")
    
    if order_columns is None:
        order_columns = ["_ingest_ts", "silver_processed_ts"]

    missing_order_columns = [c for c in order_columns if c not in df.columns]
    if missing_order_columns:
        raise ValueError(f"Order columns missing from df: {missing_order_columns}")
    
    window_spec = Window.partitionBy(*key_columns).orderBy(
        *[col(c).desc_nulls_last() for c in order_columns]
    )

    return (
        df
        .withColumn("_rn", row_number().over(window_spec))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )