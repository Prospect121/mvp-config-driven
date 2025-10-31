"""CLI principal para datacore."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import yaml

from datacore.config.validation import validate_config
from datacore.core.engine import run_layer_plan
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
    results = run_layer_plan(
        layer=args.layer,
        config=data,
        platform_name=args.platform,
        environment=args.env,
        dry_run=args.dry_run,
        fail_fast=args.fail_fast,
    )
    if args.dry_run:
        payload = {"run_id": results["run_id"], "plan": results["datasets"]}
        print(json.dumps(payload, indent=2, default=str))
    else:
        LOGGER.info("Resultados de ejecución: %s", results)


def cmd_plan(args: argparse.Namespace) -> None:
    data = _load_config(args.config)
    validate_config(data)
    layers = sorted({d.get("layer") for d in data.get("datasets", [])})
    plans: list[dict[str, Any]] = []
    run_ids: dict[str, str] = {}
    for layer in layers:
        plan_result = run_layer_plan(
            layer=layer,
            config=data,
            platform_name=args.platform,
            environment=args.env,
            dry_run=True,
        )
        run_ids[layer] = plan_result["run_id"]
        plans.extend(plan_result["datasets"])
    print(json.dumps({"run_ids": run_ids, "datasets": plans}, indent=2, default=str))


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
    run_parser.add_argument("--platform", required=False, help="Plataforma cloud (azure/aws/gcp)")
    run_parser.add_argument("--env", required=False, help="Entorno (dev/test/prod)")
    run_parser.add_argument("--log-level", default="INFO", help="Nivel de logging")
    run_parser.add_argument("--dry-run", action="store_true", help="Solo genera el plan")
    run_parser.add_argument("--fail-fast", action="store_true", help="Detener al primer error")
    run_parser.set_defaults(func=cmd_run)

    plan_parser = subparsers.add_parser("plan", help="Muestra el plan de datasets")
    plan_parser.add_argument("--config", required=True, help="Archivo YAML del proyecto")
    plan_parser.add_argument("--platform", required=False, help="Plataforma cloud (azure/aws/gcp)")
    plan_parser.add_argument("--env", required=False, help="Entorno (dev/test/prod)")
    plan_parser.set_defaults(func=cmd_plan)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main()
