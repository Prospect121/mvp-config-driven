"""CLI principal para datacore."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from datacore.config.validation import validate_config
from datacore.core.engine import run_layer_plan
from datacore.providers import Provider, load_provider
from datacore.utils import observability
from datacore.utils.logging import configure_logging

LOGGER = logging.getLogger(__name__)


def _load_config(path: str) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de configuración: {path}")
    with config_path.open("r", encoding="utf-8") as fp:
        return yaml.safe_load(fp)


def cmd_validate(args: argparse.Namespace) -> None:
    data = _load_config(args.config)
    validate_config(data, args.schema)
    LOGGER.info("Configuración válida: %s", args.config)


def cmd_run(args: argparse.Namespace) -> None:
    data = _load_config(args.config)
    validate_config(data)
    configure_logging(level=args.log_level)
    provider = load_provider(data)
    results = run_layer_plan(
        layer=args.layer,
        config=data,
        platform_name=args.platform,
        environment=args.env,
        dry_run=args.dry_run,
        fail_fast=args.fail_fast,
    )
    fabric_items = _collect_fabric_items(provider, data, force=getattr(args, "provision", False))
    _attach_fabric_items(results, fabric_items)
    summary = observability.summarize_run(results.get("datasets", []))
    LOGGER.info(
        "Resumen de ejecución",
        extra={
            **summary,
            "run_id": results.get("run_id"),
            "created_at": results.get("created_at"),
            "provider": getattr(provider, "name", "none"),
        },
    )
    if args.dry_run:
        payload = {
            "run_id": results["run_id"],
            "created_at": results.get("created_at"),
            "datasets": results["datasets"],
            "plan": results["datasets"],
        }
        if fabric_items:
            _attach_fabric_items(payload, fabric_items)
        print(json.dumps(payload, indent=2, default=str))
    else:
        LOGGER.info(
            "Resultados de ejecución",
            extra={
                "run_id": results.get("run_id"),
                "created_at": results.get("created_at"),
                "datasets": len(results.get("datasets", [])),
            },
        )


def cmd_plan(args: argparse.Namespace) -> None:
    data = _load_config(args.config)
    validate_config(data)
    provider = load_provider(data)
    layers = sorted({d.get("layer") for d in data.get("datasets", []) if d.get("layer")})
    aggregated_datasets: list[dict[str, Any]] = []
    aggregated_plan_nodes: list[dict[str, Any]] = []
    run_id: str | None = None
    created_at: str | None = None
    for layer in layers:
        plan_result = run_layer_plan(
            layer=layer,
            config=data,
            platform_name=args.platform,
            environment=args.env,
            dry_run=True,
            fail_fast=args.fail_fast,
        )
        run_id = run_id or plan_result.get("run_id")
        created_at = created_at or plan_result.get("created_at")
        datasets = plan_result.get("datasets", [])
        aggregated_datasets.extend(datasets)
        for dataset_plan in datasets:
            nodes = dataset_plan.get("plan")
            if nodes:
                aggregated_plan_nodes.append(
                    {"dataset": dataset_plan.get("name"), "nodes": nodes}
                )
    if run_id is None:
        run_id = observability.new_run_id()
    payload: dict[str, Any] = {
        "run_id": run_id,
        "created_at": created_at or datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "datasets": aggregated_datasets,
    }
    if aggregated_plan_nodes:
        payload["plan"] = aggregated_plan_nodes
    fabric_items = _collect_fabric_items(provider, data, force=getattr(args, "provision", False))
    _attach_fabric_items(payload, fabric_items)
    print(json.dumps(payload, indent=2, default=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prodi", description="CLI Datacore")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate", help="Valida un archivo de configuración")
    validate_parser.add_argument("--config", required=True, help="Ruta del archivo YAML a validar")
    validate_parser.add_argument(
        "--schema",
        required=False,
        choices=["project", "layer"],
        help="Forzar un esquema específico",
    )
    validate_parser.set_defaults(func=cmd_validate)

    run_parser = subparsers.add_parser("run", help="Ejecuta una capa")
    run_parser.add_argument(
        "--layer",
        required=True,
        choices=["raw", "bronze", "silver", "gold"],
        help="Capa a ejecutar",
    )
    run_parser.add_argument("--config", required=True, help="Archivo YAML de configuración")
    run_parser.add_argument(
        "--platform",
        required=False,
        help="Plataforma cloud (azure/aws/gcp/fabric)",
    )
    run_parser.add_argument("--env", required=False, help="Entorno (dev/test/prod)")
    run_parser.add_argument("--log-level", default="INFO", help="Nivel de logging")
    run_parser.add_argument("--dry-run", action="store_true", help="Solo genera el plan")
    run_parser.add_argument("--fail-fast", action="store_true", help="Detener al primer error")
    run_parser.add_argument(
        "--provision",
        action="store_true",
        help="Provisiona recursos opcionales del provider (Fabric)",
    )
    run_parser.set_defaults(func=cmd_run)

    plan_parser = subparsers.add_parser("plan", help="Muestra el plan de datasets")
    plan_parser.add_argument("--config", required=True, help="Archivo YAML del proyecto")
    plan_parser.add_argument(
        "--platform",
        required=False,
        help="Plataforma cloud (azure/aws/gcp/fabric)",
    )
    plan_parser.add_argument("--env", required=False, help="Entorno (dev/test/prod)")
    plan_parser.add_argument("--fail-fast", action="store_true", help="Detener al primer error")
    plan_parser.add_argument(
        "--provision",
        action="store_true",
        help="Provisiona recursos opcionales del provider (Fabric)",
    )
    plan_parser.set_defaults(func=cmd_plan)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main()
def _infer_default_fabric_items(dataset_cfg: dict[str, Any]) -> list[str]:
    inferred: list[str] = []
    source = dataset_cfg.get("source")
    if isinstance(source, dict) and source.get("type") == "shortcut":
        inferred.append("shortcut")
    sink = dataset_cfg.get("sink", {})
    if isinstance(sink, dict) and sink.get("backend") == "delta":
        inferred.append("lakehouse")
    fabric_cfg = dataset_cfg.get("orchestration", {}).get("fabric", {})
    if fabric_cfg.get("spark_job"):
        inferred.append("spark_job")
    if fabric_cfg.get("pipeline"):
        inferred.append("pipeline")
    return inferred


def _ensure_fabric_items(
    provider: Provider,
    dataset_cfg: dict[str, Any],
    *,
    force: bool,
) -> list[dict[str, Any]]:
    fabric_cfg = dataset_cfg.get("orchestration", {}).get("fabric", {})
    create_items: list[str] = list(fabric_cfg.get("create_items", []))
    if not create_items and force:
        create_items = _infer_default_fabric_items(dataset_cfg)
    if not create_items:
        return []

    # Evitamos duplicados manteniendo orden
    ordered_unique: list[str] = []
    for item in create_items:
        if item not in ordered_unique:
            ordered_unique.append(item)

    created: list[dict[str, Any]] = []
    source_cfg = dataset_cfg.get("source")
    sink_cfg = dataset_cfg.get("sink") or {}

    for item in ordered_unique:
        if item == "shortcut" and provider.supports("shortcuts"):
            if isinstance(source_cfg, dict) and source_cfg.get("type") == "shortcut":
                provider.ensure_shortcut(source_cfg)
                from datacore.providers.fabric.shortcuts import fingerprint

                created.append(
                    {
                        "type": "shortcut",
                        "name": source_cfg.get("name"),
                        "fingerprint": fingerprint(source_cfg),
                    }
                )
        elif item == "spark_job" and provider.supports("orchestration"):
            job_cfg: dict[str, Any] = {
                "name": f"{dataset_cfg.get('name', 'dataset')}-spark-job",
                **fabric_cfg.get("spark_job", {}),
            }
            metadata = provider.ensure_spark_job(job_cfg)
            metadata = {k: v for k, v in (metadata or {}).items() if v is not None}
            metadata.setdefault("name", job_cfg.get("name"))
            metadata["type"] = "spark_job"
            created.append(metadata)
        elif item == "pipeline" and provider.supports("orchestration"):
            pipeline_cfg: dict[str, Any] = {
                "name": f"{dataset_cfg.get('name', 'dataset')}-pipeline",
                **fabric_cfg.get("pipeline", {}),
            }
            metadata = provider.ensure_pipeline(pipeline_cfg)
            metadata = {k: v for k, v in (metadata or {}).items() if v is not None}
            metadata.setdefault("name", pipeline_cfg.get("name"))
            metadata["type"] = "pipeline"
            created.append(metadata)
        elif item == "lakehouse" and provider.supports("lakehouse"):
            if isinstance(sink_cfg, dict):
                created.append(
                    {
                        "type": "lakehouse",
                        "name": sink_cfg.get("uri"),
                        "backend": sink_cfg.get("backend"),
                    }
                )
    return created


def _collect_fabric_items(
    provider: Provider | None,
    config: dict[str, Any],
    *,
    force: bool,
) -> list[dict[str, Any]]:
    if not provider:
        return []
    dataset_map = {
        dataset.get("name"): dataset
        for dataset in config.get("datasets", [])
        if isinstance(dataset, dict) and dataset.get("name")
    }
    collected: list[dict[str, Any]] = []
    for dataset_cfg in dataset_map.values():
        items = _ensure_fabric_items(provider, dataset_cfg, force=force)
        if items:
            collected.extend(items)
    return collected


def _attach_fabric_items(payload: dict[str, Any], items: list[dict[str, Any]]) -> None:
    if not items:
        return
    fabric_section = payload.setdefault("fabric", {})
    fabric_items: list[dict[str, Any]] = fabric_section.setdefault("items", [])
    fabric_items.extend(items)
