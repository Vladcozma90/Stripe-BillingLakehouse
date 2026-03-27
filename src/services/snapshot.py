from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame

def merge_current_snapshot(
        spark: SparkSession,
        current_path: str,
        current_table: str,
        df: DataFrame,
        pk: str,
) -> None:
    if not DeltaTable.isDeltaTable(spark, current_path):
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(current_path)
        )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {current_table} USING DELTA LOCATION '{current_path}'")

    current_dt = DeltaTable.forName(spark, current_table)

    (current_dt.alias("t")
     .merge(df.alias("s"), f"t.{pk} = s.{pk}")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
     )