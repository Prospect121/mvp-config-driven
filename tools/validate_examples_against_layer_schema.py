"""Valida los ejemplos YAML contra `layer.schema.json`."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Iterator, Tuple

import yaml
from jsonschema import Draft7Validator


def _wrap_dataset(document: dict) -> dict:
    dataset = dict(document)
    name = dataset.pop("dataset", dataset.get("name", "example"))
    dataset["name"] = name
    project = dataset.pop("project", "examples")
    environment = dataset.pop("environment", "dev")
    platform = dataset.pop("platform", "local")
    return {
        "project": project,
        "environment": environment,
        "platform": platform,
        "datasets": [dataset],
    }


def _iter_documents(base_dir: Path) -> Iterator[Tuple[Path, int, dict]]:
    paths = sorted(base_dir.rglob("*.yml")) + sorted(base_dir.rglob("*.yaml"))
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for index, document in enumerate(yaml.safe_load_all(handle), start=1):
                if not isinstance(document, dict) or not document:
                    continue
                if "datasets" in document:
                    yield path, index, document
                elif "dataset" in document or "name" in document:
                    yield path, index, _wrap_dataset(document)


def _format_error_path(error) -> str:
    segments = [str(part) for part in error.path]
    return "/".join(segments) if segments else "<root>"


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    schema_path = repo_root / "datacore" / "config" / "schemas" / "layer.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = Draft7Validator(schema)
    examples_dir = repo_root / "examples"
    has_errors = False
    for path, index, config in _iter_documents(examples_dir):
        errors = sorted(validator.iter_errors(config), key=lambda err: list(err.path))
        for error in errors:
            has_errors = True
            location = _format_error_path(error)
            sys.stderr.write(
                f"{path} [doc #{index}] {location}: {error.message}\n"
            )
    return 1 if has_errors else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
