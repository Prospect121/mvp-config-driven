from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Tuple

from datacore.context import PipelineContext, build_context, ensure_spark, stop_spark
from datacore.io import build_storage_adapter, read_df, write_df

try:  # pragma: no cover - pyspark optional in unit tests
    from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - pyspark optional in CI
    SparkDataFrame = None  # type: ignore

try:  # pragma: no cover - optional dependency when using pandas fallbacks
    import pandas as _pd  # type: ignore
except Exception:  # pragma: no cover - pandas optional
    _pd = None  # type: ignore


def _deep_merge(base: Mapping[str, Any] | None, extra: Mapping[str, Any] | None) -> Dict[str, Any]:
    result: Dict[str, Any] = dict(base or {})
    for key, value in (extra or {}).items():
        if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            result[key] = _deep_merge(result[key], value)  # type: ignore[arg-type, assignment]
        else:
            result[key] = value  # type: ignore[assignment]
    return result


def _looks_like_spark(engine: Any) -> bool:
    if engine is None:
        return False
    if SparkDataFrame is None:
        return hasattr(engine, "createDataFrame") and hasattr(engine, "read")
    return hasattr(engine, "createDataFrame") and hasattr(engine, "read")


def _normalize_compute(context: PipelineContext) -> Dict[str, Any]:
    dataset_layers = (context.dataset_cfg.get("layers") or {}) if context.dataset_cfg else {}
    dataset_raw = dataset_layers.get("raw") or {}
    dataset_compute = dataset_raw.get("compute") or {}
    runtime_compute = context.layer_config.compute or {}
    return _deep_merge(dataset_compute, runtime_compute)


def _normalize_layer_sections(
    context: PipelineContext,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    dataset_layers = (context.dataset_cfg.get("layers") or {}) if context.dataset_cfg else {}
    dataset_raw = dataset_layers.get("raw") or {}

    dataset_io = dataset_raw.get("io") or {}
    runtime_io = context.layer_config.io or {}
    source_cfg = _deep_merge(dataset_io.get("source"), runtime_io.get("source"))
    sink_cfg = _deep_merge(dataset_io.get("sink"), runtime_io.get("sink"))

    transform_cfg = _deep_merge(dataset_raw.get("transform"), context.layer_config.transform)
    dq_cfg = _deep_merge(dataset_raw.get("dq"), context.layer_config.dq)
    storage_cfg = _deep_merge(dataset_raw.get("storage"), context.layer_config.storage)

    return source_cfg, sink_cfg, transform_cfg, dq_cfg, storage_cfg


def _preferred_protocol(cfg: Mapping[str, Any]) -> str | None:
    for key in ("protocol", "preferred_protocol", "scheme"):
        value = cfg.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _join_uri(base: str, suffix: str) -> str:
    if not base:
        return suffix
    if not suffix:
        return base
    if base.endswith("/"):
        return base + suffix.lstrip("/")
    return base.rstrip("/") + "/" + suffix.lstrip("/")


def _resolve_uri(
    cfg: Mapping[str, Any],
    *,
    storage_cfg: Mapping[str, Any],
    default_local: bool = False,
) -> str:
    if "uri" in cfg and cfg["uri"]:
        return str(cfg["uri"])
    if "path" in cfg and cfg["path"]:
        return str(cfg["path"])

    fallback = cfg.get("local_fallback")
    if fallback and default_local:
        return str(fallback)

    preferred = _preferred_protocol(cfg)
    uris_cfg = cfg.get("uris") if isinstance(cfg.get("uris"), Mapping) else {}

    if preferred and preferred in uris_cfg:
        return str(uris_cfg[preferred])

    for protocol, protocol_cfg in (storage_cfg or {}).items():
        if preferred and protocol != preferred:
            continue
        candidate = (protocol_cfg or {}).get("default_uri")
        if candidate:
            suffix = cfg.get("relative_path") or cfg.get("path_suffix")
            return _join_uri(str(candidate), str(suffix)) if suffix else str(candidate)

    if uris_cfg:
        ordered = [key for key in ("file", "local", "s3", "abfss", "gs") if key in uris_cfg]
        if not ordered:
            ordered = list(uris_cfg.keys())
        chosen = ordered[0]
        return str(uris_cfg[chosen])

    if fallback:
        return str(fallback)

    raise ValueError("No URI available for raw IO entry")


def _read_sources(
    engine: Any,
    source_cfg: Mapping[str, Any] | Iterable[Mapping[str, Any]],
    env_cfg: Mapping[str, Any],
    storage_cfg: Mapping[str, Any],
):
    entries: Iterable[Mapping[str, Any]]
    if isinstance(source_cfg, Mapping):
        if not source_cfg:
            raise ValueError("Raw layer requires an 'io.source' configuration")
        entries = [source_cfg]
    else:
        entries = list(source_cfg)
        if not entries:
            raise ValueError("Raw layer requires at least one source entry")

    spark = engine if _looks_like_spark(engine) else None
    frames = []

    for entry in entries:
        if not isinstance(entry, Mapping):
            raise TypeError("Raw source configuration entries must be mappings")

        prefer_local = bool(entry.get("use_local_fallback"))
        uri = _resolve_uri(entry, storage_cfg=storage_cfg, default_local=prefer_local)
        adapter = build_storage_adapter(uri, env_cfg, entry.get("filesystem"))
        fmt = str(entry.get("format") or entry.get("type") or "parquet")
        reader_options = adapter.merge_reader_options(entry.get("options"))
        df = read_df(
            uri=adapter.uri,
            fmt=fmt,
            spark=spark,
            engine="spark" if spark is not None else "auto",
            storage_options=adapter.storage_options,
            reader_options=reader_options,
        )
        frames.append(df)

    if not frames:
        raise ValueError("Raw layer requires at least one source")

    if len(frames) == 1:
        return frames[0]

    if spark is not None:
        result = frames[0]
        for frame in frames[1:]:
            result = result.unionByName(frame)
        return result

    if _pd is None:
        raise TypeError("Unable to merge multiple raw sources without pandas support")

    pdfs = []
    for frame in frames:
        if hasattr(frame, "toPandas"):
            pdfs.append(frame.toPandas())
        elif isinstance(frame, _pd.DataFrame):  # type: ignore[arg-type]
            pdfs.append(frame)
        else:
            try:
                pdfs.append(frame.to_pandas())  # type: ignore[attr-defined]
            except Exception as exc:  # pragma: no cover - defensive
                raise TypeError("Unsupported frame type for concatenation") from exc
    return _pd.concat(pdfs, ignore_index=True)


def _call_with_compatible_args(func: Any, *args: Any) -> Any:
    from inspect import signature

    sig = signature(func)
    params = list(sig.parameters.values())

    if any(p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD) for p in params):
        return func(*args)

    usable = args[: len(params)]
    return func(*usable)


def _apply_transforms(engine: Any, df: Any, transform_cfg: Mapping[str, Any]) -> Any:
    if not transform_cfg:
        return df

    functions = transform_cfg.get("functions") or []
    for func in functions:
        if callable(func):
            df = _call_with_compatible_args(func, df, engine, transform_cfg)

    sql_cfg = transform_cfg.get("sql")
    if sql_cfg:
        spark = engine if _looks_like_spark(engine) else None
        if spark is None:
            raise ValueError("SQL transforms require a Spark engine")
        statements = sql_cfg if isinstance(sql_cfg, Iterable) and not isinstance(sql_cfg, str) else [sql_cfg]
        view_name = str(transform_cfg.get("temp_view") or "raw_input")
        if hasattr(df, "createOrReplaceTempView"):
            df.createOrReplaceTempView(view_name)
        else:
            raise ValueError("Engine does not support temp views for SQL transforms")
        result = df
        for stmt in statements:
            if not stmt:
                continue
            result = spark.sql(str(stmt))
        df = result

    return df


def _apply_dq(engine: Any, df: Any, dq_cfg: Mapping[str, Any]) -> Any:
    if not dq_cfg:
        return df

    checks = dq_cfg.get("checks") or []
    for check in checks:
        if not callable(check):
            continue
        result = _call_with_compatible_args(check, df, engine, dq_cfg)
        if result is False:
            raise ValueError("Raw data-quality check failed")
        if result not in (None, True):
            df = result

    expectations = dq_cfg.get("expectations") or {}
    if expectations:
        min_rows = expectations.get("min_row_count")
        if min_rows is not None:
            row_count: int | None = None
            if hasattr(df, "count") and callable(getattr(df, "count")):
                count_result = df.count()
                if isinstance(count_result, (int, float)):
                    row_count = int(count_result)
                else:
                    try:
                        row_count = int(getattr(count_result, "max")())  # type: ignore[call-arg]
                    except Exception:
                        try:
                            row_count = int(len(df))  # type: ignore[arg-type]
                        except Exception as exc:  # pragma: no cover - fallback guard
                            raise ValueError("Unable to determine row count for expectations") from exc
            elif hasattr(df, "__len__"):
                row_count = int(len(df))  # type: ignore[arg-type]
            elif hasattr(df, "shape"):
                row_count = int(df.shape[0])
            else:
                raise ValueError("Unable to determine row count for expectations")
            if row_count < int(min_rows):
                raise ValueError(
                    f"Raw data-quality check failed: expected at least {min_rows} rows, found {row_count}"
                )

    return df


def _write_sink(
    engine: Any,
    df: Any,
    sink_cfg: Mapping[str, Any],
    env_cfg: Mapping[str, Any],
    storage_cfg: Mapping[str, Any],
):
    if not sink_cfg:
        return df

    spark = engine if _looks_like_spark(engine) else None

    if sink_cfg.get("kind") == "noop":
        return df

    table = sink_cfg.get("table") or sink_cfg.get("name")
    mode = str(sink_cfg.get("mode") or "overwrite")
    fmt = sink_cfg.get("format") or sink_cfg.get("type") or "parquet"
    options = sink_cfg.get("options") or {}

    if table:
        if spark is None:
            raise ValueError("Table sinks require a Spark session")
        writer = df.write
        if fmt:
            writer = writer.format(str(fmt))
        if options:
            writer = writer.options(**options)
        writer.mode(mode).saveAsTable(str(table))
        return df

    uri = _resolve_uri(sink_cfg, storage_cfg=storage_cfg)
    adapter = build_storage_adapter(uri, env_cfg, sink_cfg.get("filesystem"))
    partition_by = sink_cfg.get("partition_by")
    coalesce = sink_cfg.get("coalesce")
    repartition = sink_cfg.get("repartition")

    write_df(
        df,
        adapter.uri,
        str(fmt),
        mode=mode,
        partition_by=partition_by,
        coalesce=coalesce,
        repartition=repartition,
        engine="spark" if spark is not None else "auto",
        storage_options=adapter.storage_options,
        writer_options=adapter.merge_writer_options(options),
    )
    return df


def execute(context: PipelineContext, engine: Any) -> Any:
    source_cfg, sink_cfg, transform_cfg, dq_cfg, storage_cfg = _normalize_layer_sections(context)

    df = _read_sources(engine, source_cfg, context.env_cfg, storage_cfg)
    df = _apply_transforms(engine, df, transform_cfg)
    df = _apply_dq(engine, df, dq_cfg)
    df = _write_sink(engine, df, sink_cfg, context.env_cfg, storage_cfg)
    return df


def _build_raw_engine(context: PipelineContext) -> Tuple[Any, Any]:
    compute_cfg = _normalize_compute(context)
    kind = (
        str(
            compute_cfg.get("kind")
            or compute_cfg.get("engine")
            or compute_cfg.get("type")
            or "spark"
        )
        .strip()
        .lower()
    )

    if kind == "stub":
        engine = compute_cfg.get("engine")
        cleanup = compute_cfg.get("cleanup")
        if callable(cleanup):
            return engine, cleanup
        return engine, lambda: None

    if kind == "existing":
        engine = compute_cfg.get("session") or compute_cfg.get("engine")
        if engine is None:
            raise ValueError("Existing compute configuration requires 'session' or 'engine'")
        cleanup = compute_cfg.get("cleanup")
        if callable(cleanup):
            return engine, cleanup
        return engine, lambda: None

    if kind != "spark":
        raise ValueError(f"Unsupported compute kind for Raw layer: {kind}")

    spark = ensure_spark(context)
    for key, value in (compute_cfg.get("options") or {}).items():
        spark.conf.set(str(key), value)
    return spark, lambda: stop_spark(context)


def run(cfg: Dict[str, Any]) -> Any:
    context = build_context(cfg)
    if context.dry_run:
        print("[raw] Dry run requested - skipping execution")
        return None

    engine, cleanup = _build_raw_engine(context)
    try:
        return execute(context, engine)
    finally:
        cleanup()
