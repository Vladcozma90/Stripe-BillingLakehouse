from pyspark.sql import SparkSession
from src.services.envs import load_envs, EnvConfig
from src.services.logger import setup_log
import logging
import os

logger = logging.getLogger(__name__)

def run_booststrap() -> None:
    env = load_envs()
    setup_log(os.getenv("LOG_LEVEL", "INFO").upper())
    spark = SparkSession.builder.appName("booststrap").getOrCreate()

    #to import job the files here

    if __name__ == "__main__":
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except Exception:
            pass
        run_booststrap()