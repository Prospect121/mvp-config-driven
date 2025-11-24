"""Valida un plan JSON contra un schema explícito."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from jsonschema import Draft7Validator


def main() -> int:
    if len(sys.argv) != 3:
        sys.stderr.write("Uso: validate_plan_against_schema.py <plan.json> <schema.json>\n")
        return 1
    plan_path = Path(sys.argv[1])
    schema_path = Path(sys.argv[2])
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(plan), key=lambda e: e.path)
    if errors:
        for error in errors:
            path = "$"
            if error.path:
                path += "." + ".".join(map(str, error.path))
            sys.stderr.write(f"[PLAN] {path}: {error.message}\n")
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
