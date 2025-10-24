from typing import Any, Dict, Optional

from pyspark.sql import SparkSession, DataFrame
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from datacore.io.fs import read_df, storage_options_from_env
from pipelines.utils.logger import get_logger


logger = get_logger("io.reader")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    before_sleep=before_sleep_log(logger, level=30),
)
def read_parquet(
    spark: SparkSession,
    path: str,
    *,
    env: Optional[Dict[str, str]] = None,
    reader_options: Optional[Dict[str, Any]] = None,
) -> DataFrame:
    """Read a Parquet dataset using the shared filesystem adapters."""

    storage_opts = storage_options_from_env(path, env or {}) if path else {}
    logger.info("reading parquet", extra={"path": path, "storage_options": bool(storage_opts)})
    return read_df(
        path,
        "parquet",
        spark=spark,
        storage_options=storage_opts,
        reader_options=reader_options or {},
    )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), before_sleep=before_sleep_log(logger, level=30))
def read_table_by_jdbc(spark: SparkSession, jdbc_url: str, table: str, properties: Dict[str, str]) -> DataFrame:
    """Read a table via JDBC using Spark's DataFrame reader."""
    logger.info("reading jdbc", extra={"url": jdbc_url, "table": table})
    return spark.read.jdbc(url=jdbc_url, table=table, properties=properties)
