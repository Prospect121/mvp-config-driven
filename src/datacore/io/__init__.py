"""IO adapters bridging Spark and fsspec-backed filesystems."""

from .adapters import StorageAdapter, build_storage_adapter
from .fs import read_df, write_df, storage_options_from_env
from .http import fetch_json_to_df
from .jdbc import read_jdbc

__all__ = [
    "StorageAdapter",
    "build_storage_adapter",
    "read_df",
    "write_df",
    "storage_options_from_env",
    "fetch_json_to_df",
    "read_jdbc",
]
