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
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

import fsspec

from .http import fetch_json_to_df
from .jdbc import read_jdbc as _read_jdbc

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

    storage_section = (env_cfg.get("storage") or {}).get(protocol, {})
    credentials_cfg = storage_section.get("credentials") or {}

    if protocol == "s3":
        access_key = credentials_cfg.get("access_key")
        if access_key is None:
            env_var = credentials_cfg.get("access_key_env") or env_cfg.get("s3a_access_key_env")
            if env_var:
                access_key = os.environ.get(str(env_var))
            else:
                access_key = os.environ.get("AWS_ACCESS_KEY_ID")

        secret_key = credentials_cfg.get("secret_key")
        if secret_key is None:
            env_var = credentials_cfg.get("secret_key_env") or env_cfg.get("s3a_secret_key_env")
            if env_var:
                secret_key = os.environ.get(str(env_var))
            else:
                secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        session_token = credentials_cfg.get("session_token")
        if session_token is None:
            env_var = credentials_cfg.get("session_token_env") or env_cfg.get("s3a_session_token_env")
            if env_var:
                session_token = os.environ.get(str(env_var))
            else:
                session_token = os.environ.get("AWS_SESSION_TOKEN")

        endpoint = credentials_cfg.get("endpoint")
        if endpoint is None:
            env_var = credentials_cfg.get("endpoint_env")
            if env_var:
                endpoint = os.environ.get(str(env_var))
        if endpoint is None:
            endpoint = env_cfg.get("s3a_endpoint") or os.environ.get("AWS_ENDPOINT_URL")

        disable_ssl_flag = credentials_cfg.get("disable_ssl")
        if disable_ssl_flag is None:
            env_var = credentials_cfg.get("disable_ssl_env")
            if env_var:
                disable_ssl_flag = os.environ.get(str(env_var))
        if disable_ssl_flag is None:
            disable_ssl_flag = env_cfg.get("s3a_disable_ssl") or os.environ.get("S3A_DISABLE_SSL")

        if _boolish(disable_ssl_flag):
            raise ValueError(
                "TLS must remain enabled for S3 connections. Remove the disable flag from the configuration."
            )

        if access_key:
            opts["key"] = access_key
        if secret_key:
            opts["secret"] = secret_key
        if session_token:
            opts["token"] = session_token

        client_kwargs: Dict[str, Any] = {}
        if endpoint:
            client_kwargs["endpoint_url"] = endpoint
        if disable_ssl_flag is not None:
            client_kwargs.setdefault("use_ssl", True)

        if client_kwargs:
            opts["client_kwargs"] = client_kwargs

    elif protocol == "abfss":
        account_name = credentials_cfg.get("account_name")
        if account_name is None:
            env_var = credentials_cfg.get("account_name_env") or env_cfg.get("abfss_account_name_env")
            if env_var:
                account_name = os.environ.get(str(env_var))
            else:
                account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")

        account_key = credentials_cfg.get("account_key")
        if account_key is None:
            env_var = credentials_cfg.get("account_key_env") or env_cfg.get("abfss_account_key_env")
            if env_var:
                account_key = os.environ.get(str(env_var))
            else:
                account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

        sas_token = credentials_cfg.get("sas_token")
        if sas_token is None:
            env_var = credentials_cfg.get("sas_token_env") or env_cfg.get("abfss_sas_token_env")
            if env_var:
                sas_token = os.environ.get(str(env_var))
            else:
                sas_token = os.environ.get("AZURE_STORAGE_SAS_TOKEN")

        tenant_id = credentials_cfg.get("tenant_id")
        if tenant_id is None:
            env_var = credentials_cfg.get("tenant_id_env") or env_cfg.get("abfss_tenant_id_env")
            if env_var:
                tenant_id = os.environ.get(str(env_var))

        client_id = credentials_cfg.get("client_id")
        if client_id is None:
            env_var = credentials_cfg.get("client_id_env") or env_cfg.get("abfss_client_id_env")
            if env_var:
                client_id = os.environ.get(str(env_var))

        client_secret = credentials_cfg.get("client_secret")
        if client_secret is None:
            env_var = credentials_cfg.get("client_secret_env") or env_cfg.get("abfss_client_secret_env")
            if env_var:
                client_secret = os.environ.get(str(env_var))

        if account_name:
            opts["account_name"] = account_name
        if account_key:
            opts["account_key"] = account_key
        if sas_token:
            opts["sas_token"] = sas_token
        if tenant_id and client_id and client_secret:
            opts["tenant_id"] = tenant_id
            opts["client_id"] = client_id
            opts["client_secret"] = client_secret

    elif protocol == "gs":
        token = credentials_cfg.get("token")
        if token is None:
            env_var = credentials_cfg.get("token_env") or env_cfg.get("gs_token_env")
            if env_var:
                token = os.environ.get(str(env_var))
            else:
                token = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        project = credentials_cfg.get("project")
        if project is None:
            env_var = credentials_cfg.get("project_env") or env_cfg.get("gs_project_env")
            if env_var:
                project = os.environ.get(str(env_var))
            else:
                project = os.environ.get("GCP_PROJECT")

        if token:
            opts["token"] = token
        if project:
            opts["project"] = project

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
    source: Any,
    fmt: Optional[str] = None,
    *,
    spark=None,
    engine: str = "auto",
    storage_options: Optional[Dict[str, Any]] = None,
    reader_options: Optional[Dict[str, Any]] = None,
):
    """Read a dataset declared either as a URI or declarative mapping."""

    metadata: Dict[str, Any] = {}

    if isinstance(source, Mapping):
        source_type = str(source.get("type") or source.get("format") or "files").lower()
        if source_type == "http":
            df, metrics = fetch_json_to_df(source, spark=spark)
            metadata["http"] = metrics
            if getattr(metrics, "watermark", None):
                metadata["watermark"] = metrics.watermark
            return df, metadata
        if source_type == "jdbc":
            df, watermark = _read_jdbc(source, spark)
            if watermark:
                metadata["watermark"] = watermark
            return df, metadata

        resolved_uri = source.get("uri") or source.get("path")
        if not resolved_uri:
            raise ValueError("File-based sources require a 'uri' or 'path'")
        merged_storage_options = dict(storage_options or {})
        merged_storage_options.update(source.get("storage_options") or {})
        merged_reader_options = dict(reader_options or {})
        merged_reader_options.update(source.get("options") or {})
        resolved_fmt = fmt or source.get("format") or source.get("type") or "parquet"
        return read_df(
            str(resolved_uri),
            str(resolved_fmt),
            spark=spark,
            engine=engine,
            storage_options=merged_storage_options,
            reader_options=merged_reader_options,
        )

    uri = str(source)
    if fmt is None:
        raise ValueError("A format must be supplied when using URI based reads")

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
            return df, metadata
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
                return spark.createDataFrame(pdf), metadata
            staged.cleanup()
            return pdf, metadata
        except Exception as exc:
            last_error = exc
    if engine in {"auto", "polars"}:
        try:
            pldf = _polars_reader(fmt_norm, local_files, local_options)
            if spark is not None:
                pdf = pldf.to_pandas()
                return spark.createDataFrame(pdf), metadata
            staged.cleanup()
            return pldf, metadata
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
