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
    option_blocks = [
        config,
        raw_partitioning,
        config.get("options", {}),
        config.get("read_options", {}),
    ]
    alias_map = {
        "partitionColumn": ["partition_column"],
        "lowerBound": ["lower_bound"],
        "upperBound": ["upper_bound"],
        "numPartitions": ["num_partitions"],
    }
    for canonical, aliases in alias_map.items():
        value = None
        for block in option_blocks:
            if canonical in block:
                value = block[canonical]
                break
            for alias in aliases:
                if alias in block:
                    value = block[alias]
                    break
            if value is not None:
                break
        if value is not None:
            partitioning[canonical] = value

    for block in option_blocks:
        if "fetchsize" in block:
            partitioning["fetchsize"] = block["fetchsize"]
            break

    return partitioning


def _collect_pushdown(config: dict[str, Any]) -> dict[str, Any]:
    pushdown_flags = {}
    blocks = [config, config.get("options", {}), config.get("read_options", {})]
    for block in blocks:
        if "pushdown" in block:
            pushdown_flags["pushDownPredicate"] = _bool_to_lower(block["pushdown"])
        if "predicate_pushdown" in block:
            pushdown_flags["pushDownPredicate"] = _bool_to_lower(block["predicate_pushdown"])
    return pushdown_flags


def read(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    options: dict[str, Any] = {}
    options.update(config.get("options", {}))
    options.update(config.get("read_options", {}))

    url = options.pop("url", config.get("url"))
    if not url:
        raise ValueError("La configuración JDBC requiere 'url'")

    table = config.get("table") or options.pop("dbtable", None)
    query = config.get("query") or options.pop("query", None)
    if query:
        table = f"({query}) tmp"
    if not table:
        raise ValueError("Se requiere 'table' o 'query' para leer vía JDBC")

    partitioning = _collect_partitioning(config)
    pushdown = _collect_pushdown(config)

    for key in [
        "partitionColumn",
        "lowerBound",
        "upperBound",
        "numPartitions",
        "fetchsize",
        "partition_column",
        "lower_bound",
        "upper_bound",
        "num_partitions",
        "pushdown",
        "predicate_pushdown",
    ]:
        options.pop(key, None)

    properties: dict[str, str] = {}
    for key, value in options.items():
        if value is None:
            continue
        if isinstance(value, bool):
            properties[key] = _bool_to_lower(value)
        else:
            properties[key] = str(value)

    fetchsize = partitioning.pop("fetchsize", None)
    if fetchsize is not None:
        properties["fetchsize"] = str(fetchsize)

    for key, value in pushdown.items():
        properties[key] = str(value)

    reader = spark.read

    if partitioning.get("partitionColumn") and all(
        bound in partitioning for bound in ("lowerBound", "upperBound", "numPartitions")
    ):
        return reader.jdbc(
            url=url,
            table=table,
            column=partitioning["partitionColumn"],
            lowerBound=partitioning["lowerBound"],
            upperBound=partitioning["upperBound"],
            numPartitions=partitioning["numPartitions"],
            properties=properties,
        )

    return reader.jdbc(url=url, table=table, properties=properties)


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
