"""Registro de transformaciones."""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame

from datacore.core import ops

_TRANSFORMS: dict[str, Callable[[DataFrame], DataFrame]] = {}


def register(name: str, func: Callable[[DataFrame], DataFrame]) -> None:
    _TRANSFORMS[name] = func


def apply_registered(df: DataFrame, names: list[str]) -> DataFrame:
    result = df
    for name in names:
        if name not in _TRANSFORMS:
            raise KeyError(f"Transformación {name} no registrada")
        result = _TRANSFORMS[name](result)
    return result


def apply_ops(df: DataFrame, operations: list[dict[str, object] | str]) -> DataFrame:
    if not operations:
        return df
    return ops.apply_ops(df, operations)
