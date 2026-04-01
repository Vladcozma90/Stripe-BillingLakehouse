from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame

def merge_current_snapshot(
        spark: SparkSession,
        current_path: str,
        current_table: str,
        df: DataFrame,
        pk: str,
) -> None:
    
    df.drop("_ingest_ts",
            "_ingest_date",
            )

    current_dt = DeltaTable.forName(spark, current_table)

    (current_dt.alias("t")
     .merge(df.alias("s"), f"t.{pk} = s.{pk}")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
     )