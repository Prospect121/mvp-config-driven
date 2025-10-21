from typing import Dict
from pyspark.sql import SparkSession
from pipelines.common import maybe_config_s3a


def configure_s3a(spark: SparkSession, path: str, env_cfg: Dict) -> None:
    """Ensure S3A is configured in Spark for the given path.

    This wraps the existing maybe_config_s3a helper for consistency.
    """
    maybe_config_s3a(spark, path or "", env_cfg)