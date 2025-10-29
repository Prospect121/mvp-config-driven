"""Conector JDBC genérico."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession


def read(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    options = {**config.get("options", {}), "url": config["url"], "dbtable": config["table"]}
    reader = spark.read.format("jdbc")
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load()


def write(df: DataFrame, config: dict[str, Any]) -> None:
    options = {**config.get("options", {}), "url": config["url"], "dbtable": config["table"]}
    writer = df.write.format("jdbc").mode(config.get("mode", "append"))
    for key, value in options.items():
        writer = writer.option(key, value)
    writer.save()
