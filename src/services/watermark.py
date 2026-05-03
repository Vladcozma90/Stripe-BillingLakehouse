from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql.functions import col, lit, max

def _get_last_watermark(
        spark: SparkSession,
        state_table: str,
        pipeline_name: str,
        dataset: str
) -> datetime:

    row = spark.sql(f"""
                    SELECT last_watermark_ts
                    FROM {state_table}
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}'
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """).collect()
    
    if (not row) or row[0]["last_watermark_ts"] is None:
        return datetime(1900, 1, 1)
    return row[0]["last_watermark_ts"]


def read_incremental_by_watermark(
        spark: SparkSession,
        source_df: DataFrame,
        state_table: str,
        pipeline_name: str,
        dataset: str,
        watermark_col: str = "_ingest_ts",
):
    last_wm = _get_last_watermark(spark, state_table, pipeline_name, dataset)
    incr_df = source_df.filter(col(watermark_col) > lit(last_wm))

    if incr_df.isEmpty():
        return incr_df, last_wm, None
    
    new_wm = incr_df.agg(max(col(watermark_col)).alias("mx")).collect()[0]["mx"]

    return incr_df, last_wm, new_wm



def upsert_watermark(
        spark: SparkSession,
        state_table: str,
        new_wm: datetime,
        pipeline_name: str,
        dataset: str,
        run_id: str
) -> None:
    wm_to_log = new_wm.isoformat(sep=" ")
    spark.sql(f"""
                MERGE INTO {state_table} t
                USING (
                    SELECT 
                    '{pipeline_name}' AS pipeline_name,
                    '{dataset}' AS dataset,
                    TIMESTAMP '{wm_to_log}' AS last_watermark_ts,
                    '{run_id}' AS updated_by_run_id
                ) s
                ON t.pipeline_name = s.pipeline_name AND t.dataset = s.dataset
                WHEN MATCHED UPDATE SET
                    t.last_watermark_ts = s.last_watermark_ts,
                    t.updated_by_run_id = s.updated_by_run_id,
                    t.updated_at = current_timestamp(),
                WHEN NOT MATCHED THEN INSERT (pipeline_name, dataset, last_watermark_ts, updated_by_run_id, updated_at)
                VALUES (s.pipeline_name, s.dataset, s.last_watermark_ts, s.updated_by_run_id, current_timestamp())
            """)