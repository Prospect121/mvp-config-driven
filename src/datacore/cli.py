from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import sys
import typer
from string import Template

import yaml

from datacore.layers.raw import main as raw_main
from datacore.layers.bronze import main as bronze_main
from datacore.layers.silver import main as silver_main
from datacore.layers.gold import main as gold_main

from datacore.config.schema import LayerRuntimeConfigModel, migrate_layer_config
from jsonschema import Draft202012Validator

from mvp_config_driven.core.resolver import resolve_hierarchical_layer_config

app = typer.Typer(help="DataCore orchestration commands")


_LAYER_RUNNERS: Dict[str, Any] = {
    "raw": raw_main.run,
    "bronze": bronze_main.run,
    "silver": silver_main.run,
    "gold": gold_main.run,
}


_FORCE_DRY_RUN_ENV = "PRODI_FORCE_DRY_RUN"

_PIPELINE_SCHEMA_PATH = Path("cfg/schemas/pipeline.schema.json")
_PIPELINE_VALIDATOR: Draft202012Validator | None = None


def _should_force_dry_run() -> bool:
    value = os.getenv(_FORCE_DRY_RUN_ENV)
    if value is None:
        return False
    return value.strip().lower() not in {"", "0", "false", "no"}


def _render_template(content: str, variables: Dict[str, str] | None) -> str:
    if not variables:
        return content
    template = Template(content)
    return template.safe_substitute(variables)


def _load_config(path: Path, variables: Dict[str, str] | None = None) -> Any:
    if not path.exists():
        raise typer.BadParameter(f"Configuration file not found: {path}")
    raw_text = path.read_text(encoding="utf-8")
    rendered = _render_template(raw_text, variables)
    return yaml.safe_load(rendered) or {}


def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if (
            isinstance(value, dict)
            and key in result
            and isinstance(result[key], dict)
        ):
            result[key] = _merge_dicts(result[key], value)  # type: ignore[arg-type]
        else:
            result[key] = value
    return result


def _resolve_extends(
    cfg: Any,
    path: Path,
    variables: Dict[str, Any] | None,
    *,
    seen: Optional[set[Path]] = None,
) -> Any:
    if not isinstance(cfg, dict):
        return cfg
    extends = cfg.get("extends")
    if not extends:
        return cfg

    base_path = Path(str(extends))
    if not base_path.is_absolute():
        base_path = (path.parent / base_path).resolve()

    seen = set() if seen is None else set(seen)
    if base_path in seen:
        raise typer.BadParameter(f"Circular extends detected for {base_path}")
    seen.add(base_path)

    base_cfg = _load_config(base_path, variables)
    base_resolved = _resolve_extends(base_cfg, base_path, variables, seen=seen)
    merged = _merge_dicts(base_resolved, {k: v for k, v in cfg.items() if k != "extends"})
    return merged


def _parse_vars(values: Iterable[str]) -> Dict[str, str]:
    parsed: Dict[str, str] = {}
    for entry in values:
        if not entry:
            continue
        key, sep, value = entry.partition("=")
        if not sep:
            raise typer.BadParameter(f"Variables must be provided as key=value, got '{entry}'")
        parsed[key.strip()] = value
    return parsed


def _normalize_layer_name(layer: str) -> str:
    normalized = (layer or "").strip().lower()
    if normalized not in _LAYER_RUNNERS:
        valid_layers = ", ".join(sorted(_LAYER_RUNNERS))
        raise typer.BadParameter(
            f"Unsupported layer '{layer}'. Expected one of {valid_layers}"
        )
    return normalized


def _resolve_hierarchy(cfg: Any) -> Any:
    if not isinstance(cfg, dict):
        return cfg
    return resolve_hierarchical_layer_config(cfg)


def _validate_normalized_cfg(
    cfg: Any, expected_layer: str, dq_fail_override: Optional[bool] = None
) -> Dict[str, Any]:
    if not isinstance(cfg, dict):
        raise typer.BadParameter("Layer configuration must be a mapping")

    normalized: Dict[str, Any] = dict(cfg)
    declared_layer = normalized.get("layer")
    if declared_layer is None:
        normalized["layer"] = expected_layer
    else:
        normalized_layer = _normalize_layer_name(str(declared_layer))
        if normalized_layer != expected_layer:
            raise typer.BadParameter(
                "Configuration layer '{declared}' does not match requested layer "
                "'{expected}'".format(declared=normalized_layer, expected=expected_layer)
            )
        normalized["layer"] = normalized_layer

    normalized["dry_run"] = bool(normalized.get("dry_run", False))

    migrated = migrate_layer_config(normalized)
    runtime_model = LayerRuntimeConfigModel.model_validate(migrated)

    runtime_layer = runtime_model.layer or expected_layer
    normalized_layer = _normalize_layer_name(str(runtime_layer))
    if normalized_layer != expected_layer:
        raise typer.BadParameter(
            "Configuration layer '{declared}' does not match requested layer "
            "'{expected}'".format(declared=normalized_layer, expected=expected_layer)
        )

    runtime_dict = runtime_model.model_dump(by_alias=True)
    runtime_dict["layer"] = normalized_layer
    runtime_dict["dry_run"] = bool(runtime_dict.get("dry_run", False))
    if dq_fail_override is not None:
        dq_section = dict(runtime_dict.get("dq") or {})
        dq_section["fail_on_error"] = bool(dq_fail_override)
        runtime_dict["dq"] = dq_section
    if _should_force_dry_run():
        if not runtime_dict["dry_run"]:
            typer.echo(
                f"[{normalized_layer}] {_FORCE_DRY_RUN_ENV}=1 - forcing dry_run execution"
            )
        runtime_dict["dry_run"] = True
    return runtime_dict


def _ensure_pipeline_validator() -> Draft202012Validator:
    global _PIPELINE_VALIDATOR
    if _PIPELINE_VALIDATOR is not None:
        return _PIPELINE_VALIDATOR
    if not _PIPELINE_SCHEMA_PATH.exists():
        raise typer.BadParameter(
            f"Pipeline schema not found: {_PIPELINE_SCHEMA_PATH}"
        )
    schema_data = yaml.safe_load(_PIPELINE_SCHEMA_PATH.read_text(encoding="utf-8"))
    if not isinstance(schema_data, dict):
        raise typer.BadParameter("Pipeline schema must be a JSON object")
    _PIPELINE_VALIDATOR = Draft202012Validator(schema_data)
    return _PIPELINE_VALIDATOR


def _validate_pipeline_config(cfg: Any, config_path: Path) -> Dict[str, Any]:
    if not isinstance(cfg, dict):
        raise typer.BadParameter("Pipeline configuration must be a mapping")
    pipeline_block = cfg.get("pipeline")
    if not isinstance(pipeline_block, dict):
        raise typer.BadParameter("Missing 'pipeline' section in configuration")
    validator = _ensure_pipeline_validator()
    errors = sorted(validator.iter_errors(cfg), key=lambda err: list(err.path))
    if errors:
        messages = [
            f"{list(error.path)}: {error.message}" if error.path else error.message
            for error in errors
        ]
        raise typer.BadParameter(
            f"Pipeline configuration {config_path} is invalid:\n" + "\n".join(messages)
        )
    return pipeline_block


def _execute_layer(layer: str, cfg: Dict[str, Any]) -> Any:
    if cfg.get("dry_run"):
        typer.echo(f"[{layer}] Dry run requested - skipping execution")
        return None

    runner = _LAYER_RUNNERS[layer]
    return runner(cfg)


@app.command()
def run_layer(
    layer: str = typer.Argument(..., help="Layer to execute"),
    config: Path = typer.Option(..., "--config", "-c", help="Path to layer configuration"),
    vars_: List[str] = typer.Option([], "--vars", help="Template variables as key=value"),
    dq_fail_on_error: Optional[bool] = typer.Option(
        None,
        "--dq-fail-on-error/--dq-no-fail-on-error",
        "--dq.fail-on-error/--dq.no-fail-on-error",
        help="Override data-quality fail-on-error behaviour",
    ),
) -> None:
    layer_name = _normalize_layer_name(layer)
    variables = _parse_vars(vars_)
    raw_config = _resolve_extends(_load_config(config, variables), config, variables)
    resolved_config = _resolve_hierarchy(raw_config)
    normalized_cfg = _validate_normalized_cfg(
        resolved_config, layer_name, dq_fail_override=dq_fail_on_error
    )
    _execute_layer(layer_name, normalized_cfg)


@app.command("run-pipeline")
def run_pipeline(
    pipeline: Path = typer.Option(..., "--pipeline", "-p", help="Pipeline declaration file"),
    vars_: List[str] = typer.Option([], "--vars", help="Template variables as key=value"),
    dq_fail_on_error: Optional[bool] = typer.Option(
        None,
        "--dq-fail-on-error/--dq-no-fail-on-error",
        "--dq.fail-on-error/--dq.no-fail-on-error",
        help="Override data-quality fail-on-error behaviour for all steps",
    ),
) -> None:
    variables = _parse_vars(vars_)
    raw_pipeline_cfg = _load_config(pipeline, variables)

    pipeline_vars: Dict[str, str] = {}
    steps: Any = raw_pipeline_cfg

    if isinstance(raw_pipeline_cfg, dict):
        if isinstance(raw_pipeline_cfg.get("pipeline"), dict):
            pipeline_block = _validate_pipeline_config(raw_pipeline_cfg, pipeline)
            steps = pipeline_block.get("steps")
            pipeline_vars_raw = pipeline_block.get("vars") or {}
            if not isinstance(pipeline_vars_raw, dict):
                raise typer.BadParameter("Pipeline 'vars' must be a mapping of strings")
            pipeline_vars = {str(k): str(v) for k, v in pipeline_vars_raw.items()}
        else:
            steps = raw_pipeline_cfg.get("steps") or raw_pipeline_cfg.get("pipeline")

    if not isinstance(steps, list):
        raise typer.BadParameter("Pipeline configuration must define a list of steps")

    if not steps:
        typer.echo("No steps defined in pipeline. Nothing to execute.")
        return

    for index, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            raise typer.BadParameter(f"Pipeline step #{index} must be a mapping")

        layer_value = step.get("layer")
        if not layer_value:
            raise typer.BadParameter(f"Pipeline step #{index} is missing 'layer'")

        layer_name = _normalize_layer_name(str(layer_value))

        config_value = step.get("config")
        if not config_value:
            raise typer.BadParameter(
                f"Pipeline step '{layer_name}' is missing 'config' entry"
            )

        config_path = Path(str(config_value))
        if not config_path.is_absolute():
            config_path = (pipeline.parent / config_path).resolve()

        step_vars_raw = step.get("vars") or {}
        if not isinstance(step_vars_raw, dict):
            raise typer.BadParameter(
                f"Pipeline step '{layer_name}' vars must be a mapping of strings"
            )

        merged_vars: Dict[str, str] = {}
        merged_vars.update(pipeline_vars)
        merged_vars.update({str(k): str(v) for k, v in step_vars_raw.items()})
        merged_vars.update(variables)

        layer_cfg = _resolve_extends(
            _load_config(config_path, merged_vars), config_path, merged_vars
        )
        resolved_cfg = _resolve_hierarchy(layer_cfg)
        normalized_cfg = _validate_normalized_cfg(
            resolved_cfg, layer_name, dq_fail_override=dq_fail_on_error
        )
        _execute_layer(layer_name, normalized_cfg)


@app.command()
def validate(
    config: Path = typer.Option(..., "--config", "-c", help="Configuration to validate"),
    vars_: List[str] = typer.Option([], "--vars", help="Template variables as key=value"),
) -> None:
    variables = _parse_vars(vars_)
    raw_cfg = _resolve_extends(_load_config(config, variables), config, variables)

    if isinstance(raw_cfg, dict) and isinstance(raw_cfg.get("pipeline"), dict):
        _validate_pipeline_config(raw_cfg, config)
        typer.echo(f"[pipeline] {config} is valid")
        return

    resolved_cfg = _resolve_hierarchy(raw_cfg)
    layer_value = resolved_cfg.get("layer") if isinstance(resolved_cfg, dict) else None
    if not layer_value:
        layer_value = "raw"
    layer_name = _normalize_layer_name(str(layer_value))
    _validate_normalized_cfg(resolved_cfg, layer_name)
    typer.echo(f"[{layer_name}] {config} is valid")


def main() -> None:
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
