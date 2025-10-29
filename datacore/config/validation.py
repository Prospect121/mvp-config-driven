"""Validación de configuraciones YAML."""

from __future__ import annotations

import json
from importlib import resources
from typing import Any

from jsonschema import Draft7Validator

SCHEMAS = {
    "project": "project.schema.json",
    "layer": "layer.schema.json",
}


def _load_schema(name: str) -> dict[str, Any]:
    schema_file = resources.files("datacore.config.schemas").joinpath(name)
    with schema_file.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def validate_config(data: dict[str, Any], forced_schema: str | None = None) -> None:
    schema_key = forced_schema or ("layer" if data.get("layer") else "project")
    if schema_key not in SCHEMAS:
        raise ValueError(f"Esquema desconocido: {schema_key}")
    schema = _load_schema(SCHEMAS[schema_key])
    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(data), key=lambda err: err.path)
    if errors:
        formatted = "; ".join(f"{'/'.join(map(str, err.path))}: {err.message}" for err in errors)
        raise ValueError(f"Configuración inválida ({schema_key}): {formatted}")
