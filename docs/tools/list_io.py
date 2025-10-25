#!/usr/bin/env python3
"""List URIs referenced in configuration files.

The script scans `config/` and `cfg/` for YAML files and reports any
strings that look like filesystem URIs. It highlights protocols that fall
outside the supported set (`s3://`, `abfss://`, `gs://`, `file://`).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator

import yaml

SUPPORTED_PROTOCOLS = {"s3", "abfss", "gs", "file", "http", "https", "jdbc"}
SCAN_ROOTS = [Path("config"), Path("cfg")]


def _iter_yaml_files() -> Iterator[Path]:
    for root in SCAN_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*.yml"):
            yield path
        for path in root.rglob("*.yaml"):
            yield path


def _extract_uris(node: Any) -> Iterable[str]:
    if isinstance(node, str) and "://" in node:
        yield node
    elif isinstance(node, dict):
        for value in node.values():
            yield from _extract_uris(value)
    elif isinstance(node, (list, tuple, set)):
        for item in node:
            yield from _extract_uris(item)


def _load_yaml(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)
    except Exception as exc:  # pragma: no cover - defensive
        return {"__error__": str(exc)}


def _normalize_protocol(uri: str) -> str:
    proto = uri.split("://", 1)[0].lower().strip()
    if ":" in proto:
        proto = proto.split(":", 1)[0]
    return proto or ""


def list_uris() -> Dict[str, Any]:
    report: Dict[str, Any] = {}
    for path in _iter_yaml_files():
        data = _load_yaml(path)
        if not data:
            continue
        entries = []
        for uri in _extract_uris(data):
            proto = _normalize_protocol(uri)
            entries.append({
                "uri": uri,
                "protocol": proto,
                "supported": proto in SUPPORTED_PROTOCOLS,
            })
        if entries:
            report[str(path)] = entries
    return report


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output the report as JSON for downstream automation.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    report = list_uris()
    if args.json:
        json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")
    else:
        if not report:
            print("No URIs found in configuration files.")
            return 0
        for file_path, entries in sorted(report.items()):
            print(f"{file_path}:")
            for entry in entries:
                status = "ok" if entry["supported"] else "!"
                print(f"  [{status}] {entry['uri']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
