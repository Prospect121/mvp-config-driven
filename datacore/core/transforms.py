"""Registro de transformaciones."""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame

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
