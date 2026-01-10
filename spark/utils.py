from typing import Dict
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "SparkApp", configs: Dict[str, str] = None) -> SparkSession:
    """Create and return a SparkSession with optional configs."""
    builder = SparkSession.builder.appName(app_name)
    if configs:
        for k, v in configs.items():
            builder = builder.config(k, v)
    return builder.getOrCreate()
