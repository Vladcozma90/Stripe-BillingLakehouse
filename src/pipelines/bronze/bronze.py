from __future__ import annotations
import uuid
import logging
from typing import Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, current_date, input_file_name, lit

from src.services.envs import EnvConfig

logger = logging.getLogger(__name__)

def _build_config(env: EnvConfig, dataset: str) -> dict[str, Any]:
    return {
        #PATHS
        "src_path": f"{env.landing_base_path}/{env.project}/{dataset}", 
        "tgt_path": f"{env.raw_base_path}/{env.project}/b_{dataset}",
        "checkpoint_path": f"{env.checkpoint_base_path}/{env.project}/bronze/{dataset}/checkpoint",
        "schema_path": f"{env.checkpoint_base_path}/{env.project}/bronze/{dataset}/schema",
        #TABLES
        "tgt_table": f"{env.catalog}.{env.project}_bronze.{dataset}",
        "run_logs_table": f"{env.catalog}.{env.project}_ops.run_logs",
    }

def _get_landing_format(env: EnvConfig, dataset: str) -> str:
    ds_cfg = (env.datasets or {}).get(dataset, {})
    format = ds_cfg.get("landing_format")
    if format not in ("parquet", "json"):
        raise ValueError(f"Dataset '{dataset}' must define landing_format parquet/json in YAML datasets registry.")
    return format

def _read_autoloader(
        spark: SparkSession,
        src_path: str,
        schema_path: str,
        format: str
) -> DataFrame:
    
    df = (spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", format)
                 .option("cloudFiles.schemaLocation", schema_path)
                 .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                 )
    
    if format == "json":
        df = (
            df
            .option("multiLine", "false")
            .option("pathGlobFilter", "*jsonl")
        )
    
    stream_df = (df
                 .load(src_path)
                 .drop("_rescued_data")
                 )
    
    return stream_df



def _sum_rows_in(query) -> int:
    progresses = getattr(query, "recentProgress", None) or []
    if progresses:
        return sum(int(p.get("numInputRows", 0)) for p in progresses)
    return int((query.lastProgress or {}).get("numInputRows", 0))



def ingest_bronze(spark: SparkSession, env: EnvConfig, dataset: str) -> None:

    if not dataset or not dataset.strip():
        raise ValueError("Dataset must be a non-empty string")
    
    if dataset not in (env.datasets or {}):
        raise ValueError(f"Dataset '{dataset}' not found in YAML datasets registry.")
    
    format = _get_landing_format(env, dataset)
    cfg = _build_config(env, dataset)
    pipeline_name = f"bronze_{dataset}"
    run_id = uuid.uuid4().hex

    logger.info("Starting bronze ingestion | dataset=%s | format=%s | run_id=%s", dataset, format, run_id)

    spark.sql(f"""
                INSERT INTO {cfg["run_logs_table"]} (pipeline_name, dataset, target_table, run_id, started_at, status)
                VALUES ('{pipeline_name}', '{dataset}', '{cfg["tgt_table"]}', '{run_id}', current_timestamp(), 'RUNNING')
            """)
    
    rows_in = 0

    try:

        stream_df = _read_autoloader(spark, cfg["src_path"], cfg["schema_path"], format)

        enriched_df = (stream_df
                       .withColumn("_ingest_ts", current_timestamp())
                       .withColumn("_ingest_date", current_date())
                       .withColumn("_file_name", input_file_name())
                       .withColumn("_source", lit(cfg["src_path"]))
                       .withColumn("_landing_format", lit(format))
                       )
        
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {cfg["tgt_table"]}
                    USING DELTA
                    LOCATION '{cfg["tgt_path"]}'
                """)
        
        query = (enriched_df.writeStream
                 .format("delta")
                 .outputMode("append")
                 .partitionBy("_ingest_date")
                 .option("checkpointLocation", cfg["checkpoint_path"])
                 .option("mergeSchema", "true")
                 .trigger(availableNow=True)
                 .start(cfg["tgt_path"])
                 )
        
        query.awaitTermination()

        rows_in = _sum_rows_in(query)

        spark.sql(f"""
                    UPDATE {cfg["run_logs_table"]}
                    SET finished_at = current_timestamp(),
                        status = 'SUCCESS',
                        rows_in = {rows_in},
                        rows_out = {rows_in}
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
                """)
        
        logger.info("Bronze ingest SUCCESS | dataset=%s | rows_in=%d | run_id=%s |", dataset, rows_in, run_id)

    
    except Exception as e:
        logger.exception("Bronze ingestion FAILED | dataset=%s | run_id=%s", dataset, run_id)
        msg = str(e).replace("'", "''")
        spark.sql(f"""
                    UPDATE {cfg["run_logs_table"]}
                    SET finished_at = current_timestamp(),
                        status = 'FAILED',
                        error_msg = '{msg}',
                        rows_in = {rows_in},
                        rows_out = 0
                    WHERE pipeline_name = '{pipeline_name}' AND dataset = '{dataset}' AND run_id = '{run_id}'
                """)
        raise