from typing import Dict

from datacore.io import build_storage_adapter


def configure_s3a(_spark, path: str, env_cfg: Dict) -> None:
    """Backward compatible shim that resolves storage adapters.

    The legacy S3A configuration relied on Spark Hadoop settings. Newer
    versions use :func:`datacore.io.build_storage_adapter` to resolve neutral
    credentials so this helper simply instantiates the adapter to trigger
    validation.
    """

    build_storage_adapter(path, env_cfg)