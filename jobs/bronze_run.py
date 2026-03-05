from __future__ import annotations

import os
import logging
from pyspark.sql import SparkSession

from src.utils.envs import load_envs
from src.utils.logger import setup_log
from pipelines.bronze import ingest_bronze

logger = logging.getLogger(__name__)

def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v or v.strip():
        raise RuntimeError(f"Missing required env var: {name}")
    return v.strip()

def main() -> None:
    env = load_envs()
    setup_log(os.getenv("LOG_LEVEL", "INFO").upper())

    dataset = _require_env("DATASET")
    spark = SparkSession.builder.appName(f"bronze_ingest_{dataset}").getOrCreate()

    try:
        ingest_bronze(spark, env, dataset)
    finally:
        spark.stop()

if __name__ == '__main__':
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
    main()

