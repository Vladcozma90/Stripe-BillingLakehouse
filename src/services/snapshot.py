from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame

def merge_current_snapshot(
        spark: SparkSession,
        current_table: str,
        df: DataFrame,
        key_columns: list[str]
) -> None:
    
    if not key_columns:
        raise ValueError("key_columns must not be empty.")
    
    missing_columns = [c for c in key_columns if c not in df.columns]

    if missing_columns:
        raise ValueError(f"Merge key columns missing from source DataFrame: {missing_columns}")
    
    merge_df = df.drop("_ingest_ts", "_ingest_date")

    merge_condition = " AND ".join([f"t.{c} = s.{c}" for c in key_columns])

    current_dt = DeltaTable.forName(spark, current_table)


    (
        current_dt.alias("t")
        .merge(merge_df.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )