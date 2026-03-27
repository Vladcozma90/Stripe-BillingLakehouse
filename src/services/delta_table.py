from pyspark.sql import SparkSession, DataFrame

def write_overwrite_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    table_path: str,
) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{table_path}'
    """)


def write_append_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    table_path: str,
) -> None:
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(table_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{table_path}'
    """)