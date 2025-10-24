"""IO adapters bridging Spark and fsspec-backed filesystems."""

from .fs import read_df, write_df, storage_options_from_env

__all__ = ["read_df", "write_df", "storage_options_from_env"]
