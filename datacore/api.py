"""API Python estable para integración de control planes externos."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from datacore.config.validation import validate_config
from datacore.core.engine import run_layer_plan
from datacore.utils import observability


def _schema_key(payload: dict[str, Any], forced_schema: str | None = None) -> str:
    return forced_schema or ("layer" if payload.get("layer") else "project")


def _layers_from_config(payload: dict[str, Any]) -> list[str]:
    layers = {dataset.get("layer") for dataset in payload.get("datasets", []) if dataset.get("layer")}
    return sorted(layers)


def validate_config_payload(
    payload: dict[str, Any],
    schema: str | None = None,
) -> dict[str, Any]:
    """Valida una configuración y devuelve metadatos del resultado."""
    validate_config(payload, schema)
    return {
        "valid": True,
        "schema": _schema_key(payload, schema),
    }


def build_plan(
    payload: dict[str, Any],
    layer: str | None = None,
    platform: str | None = None,
    env: str | None = None,
    fail_fast: bool = False,
) -> dict[str, Any]:
    """Genera plan para una capa o para todo el proyecto."""
    validate_config(payload)

    if layer:
        return run_layer_plan(
            layer=layer,
            config=payload,
            platform_name=platform,
            environment=env,
            dry_run=True,
            fail_fast=fail_fast,
        )

    aggregated_datasets: list[dict[str, Any]] = []
    aggregated_plan_nodes: list[dict[str, Any]] = []
    run_id: str | None = None
    created_at: str | None = None

    for current_layer in _layers_from_config(payload):
        layer_result = run_layer_plan(
            layer=current_layer,
            config=payload,
            platform_name=platform,
            environment=env,
            dry_run=True,
            fail_fast=fail_fast,
        )
        run_id = run_id or layer_result.get("run_id")
        created_at = created_at or layer_result.get("created_at")
        datasets = layer_result.get("datasets", [])
        aggregated_datasets.extend(datasets)
        for dataset_plan in datasets:
            nodes = dataset_plan.get("plan")
            if nodes:
                aggregated_plan_nodes.append(
                    {
                        "dataset": dataset_plan.get("name"),
                        "nodes": nodes,
                    }
                )

    if run_id is None:
        run_id = observability.new_run_id()

    response: dict[str, Any] = {
        "run_id": run_id,
        "created_at": created_at or datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "datasets": aggregated_datasets,
    }
    if aggregated_plan_nodes:
        response["plan"] = aggregated_plan_nodes
    return response


def run_pipeline(
    payload: dict[str, Any],
    layer: str,
    platform: str | None = None,
    env: str | None = None,
    fail_fast: bool = False,
) -> dict[str, Any]:
    """Ejecuta una capa del pipeline usando la configuración proporcionada."""
    validate_config(payload)
    return run_layer_plan(
        layer=layer,
        config=payload,
        platform_name=platform,
        environment=env,
        dry_run=False,
        fail_fast=fail_fast,
    )
