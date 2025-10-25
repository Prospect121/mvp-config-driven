from __future__ import annotations

import inspect
from typing import Any, Callable, Dict, Iterable, List, Mapping, Tuple

try:  # pragma: no cover - pyspark optional
    from pyspark.sql import SparkSession
except ModuleNotFoundError:  # pragma: no cover - pyspark optional
    SparkSession = None  # type: ignore

from datacore.quality import apply_expectations

def _call_with_compatible_args(func: Callable[..., Any], *args: Any) -> Any:
    signature = inspect.signature(func)
    parameters = list(signature.parameters.values())

    if any(param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD) for param in parameters):
        return func(*args)

    usable_args = args[: len(parameters)]
    return func(*usable_args)


def build_silver_engine(compute_cfg: Dict[str, Any] | None) -> Tuple[Any, Callable[[], None]]:
    compute_cfg = compute_cfg or {}
    kind = (compute_cfg.get("kind") or "spark").lower()

    if kind == "stub":
        engine = compute_cfg.get("engine")
        if engine is None:
            raise ValueError("Silver compute configuration requires an 'engine' when kind='stub'")
        cleanup = compute_cfg.get("cleanup")
        if callable(cleanup):
            return engine, cleanup  # pragma: no cover
        return engine, lambda: None

    if kind == "existing":
        engine = compute_cfg.get("session")
        if engine is None:
            raise ValueError("Silver compute configuration missing 'session' for kind='existing'")
        return engine, lambda: None

    if kind != "spark":
        raise ValueError(f"Unsupported compute kind for Silver layer: {kind}")

    if SparkSession is None:
        raise RuntimeError("pyspark is required to build a Spark session for the Silver layer")

    app_name = compute_cfg.get("app_name", "datacore-silver")
    builder = SparkSession.builder.appName(app_name)

    for key, value in (compute_cfg.get("options") or {}).items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()

    def _cleanup() -> None:
        session.stop()

    return session, _cleanup


def read_silver_source(engine: Any, source_cfg: Dict[str, Any] | None) -> Any:
    source_cfg = source_cfg or {}
    kind = (source_cfg.get("kind") or "spark").lower()

    if kind == "stub":
        if "loader" in source_cfg and callable(source_cfg["loader"]):
            return _call_with_compatible_args(source_cfg["loader"], engine, source_cfg)

        if "data" in source_cfg:
            data = source_cfg["data"]
            creator = source_cfg.get("factory") or getattr(engine, "createDataFrame", None)
            if callable(creator):
                schema = source_cfg.get("schema")
                return creator(data, schema) if schema is not None else creator(data)
            return data

        raise ValueError("Silver source configuration requires 'loader' or 'data' when kind='stub'")

    if kind != "spark":
        raise ValueError(f"Unsupported Silver source kind: {kind}")

    if not hasattr(engine, "read"):
        raise ValueError("Engine does not expose a Spark reader for Silver source")

    reader = engine.read
    fmt = source_cfg.get("format")
    if fmt:
        reader = reader.format(fmt)

    for key, value in (source_cfg.get("options") or {}).items():
        reader = reader.option(key, value)

    path = source_cfg.get("path")
    table = source_cfg.get("table")

    if path:
        return reader.load(path)
    if table:
        return reader.table(table)

    raise ValueError("Silver source configuration requires 'path' or 'table'")


def apply_silver_transforms(engine: Any, df: Any, transform_cfg: Dict[str, Any] | None) -> Any:
    transform_cfg = transform_cfg or {}
    functions: Iterable[Callable[..., Any]] = transform_cfg.get("functions", []) or []

    for func in functions:
        if not callable(func):
            continue
        df = _call_with_compatible_args(func, df, engine, transform_cfg)

    return df


def apply_silver_dq(engine: Any, df: Any, dq_cfg: Dict[str, Any] | None) -> Any:
    dq_cfg = dq_cfg or {}
    checks: Iterable[Callable[..., Any]] = dq_cfg.get("checks", []) or []

    for check in checks:
        if not callable(check):
            continue
        result = _call_with_compatible_args(check, df, engine, dq_cfg)
        if result is False:
            raise ValueError("Silver data-quality check failed")
        if result not in (None, True):
            df = result

    expectations = dq_cfg.get("expectations") if dq_cfg else None
    expectation_list: List[Mapping[str, Any]] = []
    if isinstance(expectations, Mapping):
        rules = expectations.get("rules")
        if isinstance(rules, Iterable) and not isinstance(rules, (str, bytes)):
            expectation_list.extend(rules)  # type: ignore[arg-type]
        else:
            expectation_list.append(expectations)
    elif isinstance(expectations, Iterable) and not isinstance(expectations, (str, bytes)):
        expectation_list.extend(expectations)  # type: ignore[arg-type]

    if expectation_list:
        apply_expectations(df, expectation_list)

    return df


def write_silver_sink(engine: Any, df: Any, sink_cfg: Dict[str, Any] | None) -> Any:
    sink_cfg = sink_cfg or {}
    kind = (sink_cfg.get("kind") or "noop").lower()

    if kind == "noop":
        return df

    if kind == "stub":
        writer = sink_cfg.get("writer")
        if callable(writer):
            _call_with_compatible_args(writer, df, engine, sink_cfg)
        return df

    if kind != "spark":
        raise ValueError(f"Unsupported Silver sink kind: {kind}")

    if not hasattr(df, "write"):
        raise ValueError("Silver sink expects a Spark DataFrame when kind='spark'")

    writer = df.write
    fmt = sink_cfg.get("format")
    if fmt:
        writer = writer.format(fmt)

    for key, value in (sink_cfg.get("options") or {}).items():
        writer = writer.option(key, value)

    mode = sink_cfg.get("mode", "append")
    path = sink_cfg.get("path")
    table = sink_cfg.get("table")

    if path:
        writer.mode(mode).save(path)
    elif table:
        writer.mode(mode).saveAsTable(table)
    else:
        raise ValueError("Silver sink configuration requires 'path' or 'table'")

    return df


def execute(cfg: Dict[str, Any], engine: Any | None = None) -> Any:
    own_engine = engine is None
    compute_cfg = cfg.get("compute") if isinstance(cfg, dict) else None
    io_cfg = (cfg.get("io") if isinstance(cfg, dict) else {}) or {}

    if engine is None:
        engine, cleanup = build_silver_engine(compute_cfg)
    else:
        cleanup = lambda: None

    try:
        df = read_silver_source(engine, io_cfg.get("source"))
        df = apply_silver_transforms(engine, df, cfg.get("transform"))
        df = apply_silver_dq(engine, df, cfg.get("dq"))
        df = write_silver_sink(engine, df, io_cfg.get("sink"))
        return df
    finally:
        if own_engine:
            cleanup()


def run(cfg: Dict[str, Any]) -> Any:
    return execute(cfg)

