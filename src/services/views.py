from pyspark.sql import SparkSession



def create_current_view(
        spark: SparkSession,
        conform_table: str,
        current_view: str,
        select_columns: list[str],
) -> None:
    
    if not select_columns:
        raise ValueError("select_columns must not be empty.")
    
    source_columns = spark.read.table(conform_table)

    missing_columns = [c for c in ["is_current", *select_columns] if c not in source_columns]

    if missing_columns:
        raise ValueError(f"Conform table '{conform_table}' is missing required columns: {missing_columns}")
    
    select_sql = ", /n".join(select_columns)

    spark.sql(f"""
                CREATE OR REPLACE VIEW {current_view} AS
                SELECT
                    {select_sql}
                FROM {conform_table}
                WHERE is_current = true
            """)


    
