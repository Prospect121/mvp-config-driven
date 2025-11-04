"""Valida los ejemplos YAML contra `layer.schema.json`."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Iterable

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


def _load_payloads(base_dir: Path) -> Iterable[tuple[Path, dict]]:
    paths = sorted(base_dir.rglob("*.yml")) + sorted(base_dir.rglob("*.yaml"))
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            documents = list(yaml.safe_load_all(handle))
        for document in documents:
            if not document:
                continue
            if "datasets" in document:
                yield path, document
            elif "dataset" in document or "name" in document:
                yield path, _wrap_dataset(document)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    schema_path = repo_root / "datacore" / "config" / "schemas" / "layer.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = Draft7Validator(schema)
    examples_dir = repo_root / "examples"
    has_errors = False
    for path, config in _load_payloads(examples_dir):
        errors = list(validator.iter_errors(config))
        if errors:
            has_errors = True
            for error in errors:
                sys.stderr.write(f"{path}: {error.message}\n")
    return 1 if has_errors else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
