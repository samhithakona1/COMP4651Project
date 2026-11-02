# Origin: original
"""SparkSession factory (local mode, single machine)."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from src.common import config


def get_spark(app_name: str = "tech-hiring", master: str | None = None,
              driver_memory: str | None = None, extra: dict | None = None) -> SparkSession:
    config.SPARK_TMP_DIR.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or config.SPARK_MASTER)
        # Only effective when the JVM is started from Python; spark-submit passes --driver-memory.
        .config("spark.driver.memory", driver_memory or config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.local.dir", str(config.SPARK_TMP_DIR))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", os.environ.get("SPARK_UI_ENABLED", "false"))
        .config("spark.sql.adaptive.enabled", "true")
    )
    for key, value in (extra or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
