"""Conector JDBC genérico."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession


def _apply_reader_options(reader, options: dict[str, Any]):
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader


def read(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    base_options: dict[str, Any] = {
        **config.get("options", {}),
        "url": config["url"],
        "dbtable": config["table"],
    }
    if "pushdown" in config:
        base_options["pushDownPredicate"] = str(config["pushdown"]).lower()
    partitioning = config.get("partitioning", {})
    for key in ["partitionColumn", "lowerBound", "upperBound", "numPartitions"]:
        if key in partitioning:
            base_options[key] = partitioning[key]
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
    if config.get("batchsize"):
        options["batchsize"] = config["batchsize"]
    if config.get("isolationLevel"):
        options["isolationLevel"] = config["isolationLevel"]
    if config.get("createTableOptions"):
        options["createTableOptions"] = config["createTableOptions"]
    if config.get("truncate"):
        mode = "overwrite"
        options["truncate"] = "true"
    writer = df.write.format("jdbc").mode(mode)
    writer = _apply_reader_options(writer, options)
    writer.save()
