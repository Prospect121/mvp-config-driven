#!/usr/bin/env python3
"""Scan repository sources for read/write operations and legacy runners."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List

SCAN_ROOTS = [Path("src"), Path("scripts")]
IO_PATTERNS = {
    "spark_read": re.compile(r"spark\.read\.(\w+)", re.IGNORECASE),
    "spark_write": re.compile(r"\.write\.(\w+)", re.IGNORECASE),
    "pandas_read": re.compile(r"read_(csv|json|parquet|table)\(", re.IGNORECASE),
    "pandas_write": re.compile(r"to_(csv|json|parquet|table)\(", re.IGNORECASE),
    "filesystem_uri": re.compile(r"(s3://|abfss://|gs://|dbfs:/|file://|wasbs://)", re.IGNORECASE),
    "jdbc_uri": re.compile(r"jdbc:[a-z0-9]+://", re.IGNORECASE),
}
LEGACY_RUNNER_PATTERNS = {
    "spark_submit": re.compile(r"spark-submit"),
    "databricks_cli": re.compile(r"databricks\s+jobs"),
    "airflow_dag": re.compile(r"AirflowDagOperator|airflow\.operators", re.IGNORECASE),
}


def iter_source_files() -> Iterable[Path]:
    for root in SCAN_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            yield path


def scan_file(path: Path) -> Dict[str, List[Dict[str, str]]]:
    findings: Dict[str, List[Dict[str, str]]] = {key: [] for key in IO_PATTERNS}
    findings.update({key: [] for key in LEGACY_RUNNER_PATTERNS})
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return findings
    for lineno, line in enumerate(lines, start=1):
        for key, pattern in IO_PATTERNS.items():
            if pattern.search(line):
                findings[key].append({"line": lineno, "content": line.strip()})
        for key, pattern in LEGACY_RUNNER_PATTERNS.items():
            if pattern.search(line):
                findings[key].append({"line": lineno, "content": line.strip()})
    return {key: value for key, value in findings.items() if value}


def build_report() -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    report: Dict[str, Dict[str, List[Dict[str, str]]]] = {}
    for path in iter_source_files():
        matches = scan_file(path)
        if matches:
            report[str(path)] = matches
    return report


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print the report in JSON format.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    report = build_report()
    if args.json:
        json.dump(report, fp=sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")
    else:
        if not report:
            print("No read/write operations detected in the scanned roots.")
            return 0
        for file_path, matches in sorted(report.items()):
            print(file_path)
            for kind, entries in matches.items():
                print(f"  [{kind}]")
                for entry in entries:
                    print(f"    L{entry['line']}: {entry['content']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
