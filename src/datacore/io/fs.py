"""Filesystem-backed DataFrame IO utilities.

This module provides high-level helpers to read and write tabular data stored on
filesystems supported by ``fsspec``. It favors Apache Spark when available, but
also supports pandas and polars as fallbacks. Remote URI schemes (``s3://``,
``s3a://``, ``abfss://``, ``gs://``) are normalized to the appropriate fsspec
filesystem implementation and credentials can be injected using the project
``env`` configuration.
"""

from __future__ import annotations

import glob
import os
import shutil
import tempfile
import weakref
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import fsspec

try:  # Optional dependency
    import pandas as pd
except Exception:  # pragma: no cover - pandas is optional for tests
    pd = None  # type: ignore

try:  # Optional dependency
    import polars as pl
except Exception:  # pragma: no cover - polars is optional for tests
    pl = None  # type: ignore


_SPARK_DF_TYPE = None
try:  # pragma: no cover - Spark may not be installed in tests
    from pyspark.sql import DataFrame as _SparkDataFrame  # type: ignore

    _SPARK_DF_TYPE = _SparkDataFrame
except Exception:  # pragma: no cover - keep optional dependency optional
    _SPARK_DF_TYPE = None


@dataclass
class _StagedData:
    """Represents a local staging area for remote files."""

    path: str
    cleanup: Callable[[], None]


def _normalize_protocol(protocol: Optional[str]) -> str:
    if not protocol:
        return "file"
    if protocol == "s3a":
        return "s3"
    return protocol


def _split_uri(uri: str) -> Tuple[str, str]:
    protocol, path = fsspec.core.split_protocol(uri)
    protocol = _normalize_protocol(protocol)
    if path.startswith("//"):
        # fsspec returns paths like //bucket/key for some schemes
        path = path[2:]
    return protocol, path


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_env_value(env_cfg: Dict[str, Any], key: str, default_env_var: str) -> Optional[str]:
    env_var_name = env_cfg.get(key)
    if env_var_name:
        return os.environ.get(env_var_name)
    return os.environ.get(default_env_var)


def storage_options_from_env(uri: str, env_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Build fsspec storage options using the existing env configuration."""

    env_cfg = env_cfg or {}
    protocol, _ = _split_uri(uri)
    opts: Dict[str, Any] = {}

    if protocol == "s3":
        access_key = _load_env_value(env_cfg, "s3a_access_key_env", "AWS_ACCESS_KEY_ID")
        secret_key = _load_env_value(env_cfg, "s3a_secret_key_env", "AWS_SECRET_ACCESS_KEY")
        session_token = _load_env_value(env_cfg, "s3a_session_token_env", "AWS_SESSION_TOKEN")
        endpoint = env_cfg.get("s3a_endpoint") or os.environ.get("AWS_ENDPOINT_URL")

        if access_key:
            opts["key"] = access_key
        if secret_key:
            opts["secret"] = secret_key
        if session_token:
            opts["token"] = session_token

        client_kwargs: Dict[str, Any] = {}
        if endpoint:
            client_kwargs["endpoint_url"] = endpoint

        disable_ssl = env_cfg.get("s3a_disable_ssl") or os.environ.get("S3A_DISABLE_SSL")
        if disable_ssl is not None:
            client_kwargs["use_ssl"] = not _boolish(disable_ssl)

        if client_kwargs:
            opts["client_kwargs"] = client_kwargs

    return opts


def _filesystem(uri: str, storage_options: Optional[Dict[str, Any]] = None):
    protocol, path = _split_uri(uri)
    fs = fsspec.filesystem(protocol, **(storage_options or {}))
    return fs, path


def _stage_to_local(fs, path: str) -> _StagedData:
    temp_dir = tempfile.mkdtemp(prefix="dcfs-")

    def cleanup() -> None:
        shutil.rmtree(temp_dir, ignore_errors=True)

    local_target: Optional[str] = None
    try:
        if hasattr(fs, "isdir") and fs.isdir(path):
            local_target = os.path.join(temp_dir, "data")
            os.makedirs(local_target, exist_ok=True)
            fs.get(path, local_target, recursive=True)
        else:
            matches: List[str] = []
            if hasattr(fs, "glob"):
                matches = fs.glob(path)
            if matches:
                local_target = os.path.join(temp_dir, "data")
                os.makedirs(local_target, exist_ok=True)
                for remote in matches:
                    basename = os.path.basename(remote.rstrip("/"))
                    if not basename:
                        basename = "part"
                    dest = os.path.join(local_target, basename)
                    fs.get(remote, dest)
            elif hasattr(fs, "exists") and fs.exists(path):
                basename = os.path.basename(path.rstrip("/")) or "part"
                local_target = os.path.join(temp_dir, basename)
                fs.get(path, local_target)
            else:
                raise FileNotFoundError(f"No data found at {path}")
    except Exception:
        cleanup()
        raise

    return _StagedData(local_target or temp_dir, cleanup)


def _spark_df_like(df: Any) -> bool:
    if _SPARK_DF_TYPE is not None and isinstance(df, _SPARK_DF_TYPE):
        return True
    return hasattr(df, "write") and hasattr(df, "repartition")


def _spark_reader(spark, fmt: str, path: str, options: Dict[str, Any]):
    reader = spark.read
    if options:
        reader = reader.options(**options)
    fmt_norm = fmt.lower()
    if fmt_norm == "parquet":
        return reader.parquet(path)
    if fmt_norm == "csv":
        return reader.csv(path)
    if fmt_norm in {"json", "jsonl"}:
        return reader.json(path)
    raise ValueError(f"Unsupported format for Spark reader: {fmt}")


def _iter_local_files(local_path: str) -> List[str]:
    if os.path.isdir(local_path):
        pattern = os.path.join(local_path, "**", "*")
        return [p for p in glob.glob(pattern, recursive=True) if os.path.isfile(p)]
    return [local_path]


def _filter_reader_options(fmt: str, options: Dict[str, Any]) -> Dict[str, Any]:
    fmt_norm = fmt.lower()
    filtered: Dict[str, Any] = {}
    for key, value in options.items():
        if fmt_norm == "csv":
            if key == "sep":
                filtered["sep"] = value
            elif key == "header":
                filtered["header"] = 0 if _boolish(value) else None
            elif key == "encoding":
                filtered["encoding"] = value
        elif fmt_norm in {"json", "jsonl"}:
            if key == "multiline":
                filtered["lines"] = not _boolish(value)
            elif key == "encoding":
                filtered["encoding"] = value
        elif fmt_norm == "parquet":
            filtered[key] = value
    if fmt_norm == "jsonl":
        filtered.setdefault("lines", True)
    return filtered


def _pandas_reader(fmt: str, files: Iterable[str], options: Dict[str, Any]):
    if pd is None:
        raise RuntimeError("pandas is not available")

    fmt_norm = fmt.lower()
    frames: List["pd.DataFrame"] = []
    for file_path in files:
        if fmt_norm == "parquet":
            frames.append(pd.read_parquet(file_path, **options))
        elif fmt_norm == "csv":
            frames.append(pd.read_csv(file_path, **options))
        elif fmt_norm in {"json", "jsonl"}:
            frames.append(pd.read_json(file_path, **options))
        else:
            raise ValueError(f"Unsupported format for pandas reader: {fmt}")
    if not frames:
        return pd.DataFrame()
    if len(frames) == 1:
        return frames[0]
    return pd.concat(frames, ignore_index=True)


def _polars_reader(fmt: str, files: Iterable[str], options: Dict[str, Any]):
    if pl is None:
        raise RuntimeError("polars is not available")

    fmt_norm = fmt.lower()
    frames: List["pl.DataFrame"] = []
    for file_path in files:
        if fmt_norm == "parquet":
            frames.append(pl.read_parquet(file_path, **options))
        elif fmt_norm == "csv":
            frames.append(pl.read_csv(file_path, **options))
        elif fmt_norm in {"json", "jsonl"}:
            frames.append(pl.read_json(file_path, **options))
        else:
            raise ValueError(f"Unsupported format for polars reader: {fmt}")
    if not frames:
        return pl.DataFrame()
    if len(frames) == 1:
        return frames[0]
    return pl.concat(frames)


def read_df(
    uri: str,
    fmt: str,
    *,
    spark=None,
    engine: str = "auto",
    storage_options: Optional[Dict[str, Any]] = None,
    reader_options: Optional[Dict[str, Any]] = None,
):
    """Read a dataset from the given URI and return a DataFrame."""

    reader_options = dict(reader_options or {})
    fs, path = _filesystem(uri, storage_options)
    fmt_norm = fmt.lower()

    last_error: Optional[Exception] = None

    if engine in {"spark", "auto"} and spark is not None:
        staged = None
        try:
            staged = _stage_to_local(fs, path)
            df = _spark_reader(spark, fmt_norm, staged.path, reader_options)
            weakref.finalize(df, staged.cleanup)
            return df
        except Exception as exc:
            if staged:
                staged.cleanup()
            if engine == "spark":
                raise
            last_error = exc

    staged = _stage_to_local(fs, path)
    local_files = _iter_local_files(staged.path)
    local_options = _filter_reader_options(fmt_norm, reader_options)

    if engine in {"auto", "pandas"}:
        try:
            pdf = _pandas_reader(fmt_norm, local_files, local_options)
            if spark is not None:
                return spark.createDataFrame(pdf)
            staged.cleanup()
            return pdf
        except Exception as exc:
            last_error = exc
    if engine in {"auto", "polars"}:
        try:
            pldf = _polars_reader(fmt_norm, local_files, local_options)
            if spark is not None:
                pdf = pldf.to_pandas()
                return spark.createDataFrame(pdf)
            staged.cleanup()
            return pldf
        except Exception as exc:
            last_error = exc

    staged.cleanup()
    if last_error:
        raise RuntimeError(f"Failed to read {uri}: {last_error}")
    raise RuntimeError(f"Failed to read {uri}: unknown error")


def _ensure_directory(fs, path: str) -> None:
    if not path:
        return
    dirname = os.path.dirname(path)
    if dirname and hasattr(fs, "makedirs"):
        fs.makedirs(dirname, exist_ok=True)


def _spark_writer(
    df: Any,
    fmt: str,
    path: str,
    fs,
    mode: str,
    partition_by: Optional[Iterable[str]],
    writer_options: Dict[str, Any],
    coalesce: Optional[int],
    repartition: Optional[int],
) -> None:
    temp_dir = tempfile.mkdtemp(prefix="dcfs-write-")
    local_path = os.path.join(temp_dir, "data")
    os.makedirs(local_path, exist_ok=True)
    try:
        tmp_df = df
        if repartition:
            tmp_df = tmp_df.repartition(repartition)
        if coalesce:
            tmp_df = tmp_df.coalesce(coalesce)

        writer = tmp_df.write.mode(mode)
        if writer_options:
            writer = writer.options(**writer_options)
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        fmt_norm = fmt.lower()
        if fmt_norm == "parquet":
            writer.parquet(local_path)
        elif fmt_norm == "csv":
            writer.csv(local_path)
        elif fmt_norm in {"json", "jsonl"}:
            writer.json(local_path)
        else:
            raise ValueError(f"Unsupported format for Spark writer: {fmt}")

        if mode == "overwrite" and hasattr(fs, "exists") and fs.exists(path):
            fs.rm(path, recursive=True)
        fs.put(local_path, path, recursive=True)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _choose_file_name(path: str, fmt: str) -> str:
    if path.endswith("/"):
        ext = {
            "parquet": ".parquet",
            "csv": ".csv",
            "json": ".json",
            "jsonl": ".jsonl",
        }.get(fmt.lower(), "")
        return path + f"part-0000{ext}"
    return path


def _pandas_writer(df: "pd.DataFrame", fmt: str, fs, path: str, mode: str, writer_options: Dict[str, Any]) -> None:
    fmt_norm = fmt.lower()
    target = _choose_file_name(path, fmt_norm)
    if mode == "overwrite" and hasattr(fs, "exists") and fs.exists(target):
        fs.rm(target)
    _ensure_directory(fs, target)

    if fmt_norm == "parquet":
        with fs.open(target, "wb") as handle:
            df.to_parquet(handle, index=False, **writer_options)
    elif fmt_norm == "csv":
        with fs.open(target, "w", newline="") as handle:
            df.to_csv(handle, index=False, **writer_options)
    elif fmt_norm in {"json", "jsonl"}:
        lines = fmt_norm == "jsonl"
        with fs.open(target, "w") as handle:
            df.to_json(handle, orient="records", lines=lines, **writer_options)
    else:
        raise ValueError(f"Unsupported format for pandas writer: {fmt}")


def _polars_writer(df: "pl.DataFrame", fmt: str, fs, path: str, mode: str, writer_options: Dict[str, Any]) -> None:
    fmt_norm = fmt.lower()
    target = _choose_file_name(path, fmt_norm)
    if mode == "overwrite" and hasattr(fs, "exists") and fs.exists(target):
        fs.rm(target)
    _ensure_directory(fs, target)

    if fmt_norm == "parquet":
        with fs.open(target, "wb") as handle:
            df.write_parquet(handle, **writer_options)
    elif fmt_norm == "csv":
        with fs.open(target, "w", newline="") as handle:
            df.write_csv(handle, **writer_options)
    elif fmt_norm in {"json", "jsonl"}:
        if pd is not None:
            _pandas_writer(df.to_pandas(), fmt, fs, path, mode, writer_options)
        else:
            raise ValueError("polars JSON writing requires pandas as dependency")
    else:
        raise ValueError(f"Unsupported format for polars writer: {fmt}")


def write_df(
    df: Any,
    uri: str,
    fmt: str,
    *,
    mode: str = "overwrite",
    partition_by: Optional[Iterable[str]] = None,
    coalesce: Optional[int] = None,
    repartition: Optional[int] = None,
    engine: str = "auto",
    storage_options: Optional[Dict[str, Any]] = None,
    writer_options: Optional[Dict[str, Any]] = None,
):
    """Write a DataFrame to a filesystem-backed destination."""

    writer_options = dict(writer_options or {})
    fs, path = _filesystem(uri, storage_options)
    fmt_norm = fmt.lower()

    last_error: Optional[Exception] = None

    if engine in {"spark", "auto"} and _spark_df_like(df):
        try:
            _spark_writer(df, fmt_norm, path, fs, mode, partition_by, writer_options, coalesce, repartition)
            return
        except Exception as exc:
            if engine == "spark":
                raise
            last_error = exc

    if pd is not None and isinstance(df, pd.DataFrame) and engine in {"auto", "pandas"}:
        _pandas_writer(df, fmt_norm, fs, path, mode, writer_options)
        return

    if pl is not None and isinstance(df, pl.DataFrame) and engine in {"auto", "polars"}:
        _polars_writer(df, fmt_norm, fs, path, mode, writer_options)
        return

    if last_error:
        raise RuntimeError(f"Failed to write {uri}: {last_error}")
    raise RuntimeError("Unsupported DataFrame type for write_df")


__all__ = [
    "read_df",
    "write_df",
    "storage_options_from_env",
]
