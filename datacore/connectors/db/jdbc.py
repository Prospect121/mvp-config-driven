"""Conector JDBC genérico."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession


def _apply_reader_options(reader, options: dict[str, Any]):
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader


def _bool_to_lower(value: Any) -> str:
    return str(bool(value)).lower()


def _collect_partitioning(config: dict[str, Any]) -> dict[str, Any]:
    partitioning: dict[str, Any] = {}
    raw_partitioning = config.get("partitioning", {})
    alias_map = {
        "partitionColumn": ["partition_column"],
        "lowerBound": ["lower_bound"],
        "upperBound": ["upper_bound"],
        "numPartitions": ["num_partitions"],
    }
    for canonical, aliases in alias_map.items():
        value = config.get(canonical)
        if value is None:
            for alias in aliases:
                if alias in config:
                    value = config[alias]
                    break
        if value is None and canonical in raw_partitioning:
            value = raw_partitioning[canonical]
        if value is None:
            for alias in aliases:
                if alias in raw_partitioning:
                    value = raw_partitioning[alias]
                    break
        if value is not None:
            partitioning[canonical] = value
    if "fetchsize" in config:
        partitioning["fetchsize"] = config["fetchsize"]
    elif "fetchsize" in raw_partitioning:
        partitioning["fetchsize"] = raw_partitioning["fetchsize"]
    return partitioning


def _collect_pushdown(config: dict[str, Any]) -> dict[str, Any]:
    pushdown_flags = {}
    if "pushdown" in config:
        pushdown_flags["pushDownPredicate"] = _bool_to_lower(config["pushdown"])
    if "predicate_pushdown" in config:
        pushdown_flags["pushDownPredicate"] = _bool_to_lower(config["predicate_pushdown"])
    options = config.get("options", {})
    if "pushdown" in options:
        pushdown_flags.setdefault("pushDownPredicate", _bool_to_lower(options["pushdown"]))
    if "predicate_pushdown" in options:
        pushdown_flags.setdefault(
            "pushDownPredicate", _bool_to_lower(options["predicate_pushdown"])
        )
    return pushdown_flags


def read(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    base_options: dict[str, Any] = {}
    base_options.update(config.get("options", {}))
    base_options.update(config.get("read_options", {}))
    base_options.setdefault("url", config["url"])
    if "table" in config:
        base_options.setdefault("dbtable", config["table"])
    if "query" in config:
        base_options.setdefault("query", config["query"])
    base_options.update(_collect_pushdown(config))
    base_options.update(_collect_partitioning(config))

    reader = spark.read.format("jdbc")
    reader = _apply_reader_options(reader, base_options)
    return reader.load()


def write(df: DataFrame, config: dict[str, Any]) -> None:
    mode = config.get("mode", "append")
    options: dict[str, Any] = {
        **config.get("options", {}),
        "url": config["url"],
        "dbtable": config["table"],
    }
    batch_size = config.get("batch_size") or config.get("batchsize")
    if batch_size:
        options["batchsize"] = batch_size
    isolation = config.get("isolation_level") or config.get("isolationLevel")
    if isolation:
        options["isolationLevel"] = isolation
    create_opts = config.get("create_table_options") or config.get("createTableOptions")
    if create_opts:
        options["createTableOptions"] = create_opts
    truncate_flag = config.get("truncate_safe") or config.get("truncate")
    if truncate_flag:
        mode = "overwrite"
        options["truncate"] = "true"
    writer = df.write.format("jdbc").mode(mode)
    writer = _apply_reader_options(writer, options)
    writer.save()
