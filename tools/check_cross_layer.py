"""Static checks preventing cross-layer imports inside ``src/datacore/layers``.

The script scans each Python module within the layer packages (raw, bronze,

silver, gold) and asserts they do not import siblings. This complements runtime

tests by failing fast during CI when an engineer accidentally introduces a

cross-layer dependency (for example, importing ``datacore.layers.bronze`` from

``layers/silver``).
"""

from __future__ import annotations

import argparse
import ast
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

LAYER_NAMES: Sequence[str] = ("raw", "bronze", "silver", "gold")


@dataclass
class Violation:
    module: Path
    lineno: int
    imported: str

    def format(self, root: Path) -> str:
        rel = self.module.relative_to(root)
        return f"{rel}:{self.lineno} imports forbidden layer '{self.imported}'"


def _iter_python_files(root: Path) -> Iterable[Path]:
    for path in sorted(root.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        yield path


def _extract_import_targets(node: ast.AST) -> Iterable[str]:
    if isinstance(node, ast.ImportFrom) and node.module:
        yield node.module
    elif isinstance(node, ast.Import):
        for alias in node.names:
            yield alias.name


def check_cross_layer(root: Path, allowlist: Sequence[str] | None = None) -> List[Violation]:
    allowlist = tuple(allowlist or ())
    root = root.resolve()

    violations: List[Violation] = []
    for path in _iter_python_files(root):
        layer_name = path.parts[path.parts.index("layers") + 1] if "layers" in path.parts else None
        if layer_name not in LAYER_NAMES:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            for target in _extract_import_targets(node):
                if target in allowlist:
                    continue
                for other in LAYER_NAMES:
                    if other == layer_name:
                        continue
                    full_other = f"datacore.layers.{other}"
                    if target == full_other or target.startswith(full_other + "."):
                        violations.append(
                            Violation(module=path, lineno=getattr(node, "lineno", 0), imported=target)
                        )
                    elif target.startswith("layers."):
                        violations.append(
                            Violation(module=path, lineno=getattr(node, "lineno", 0), imported=target)
                        )
    return violations


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Detect cross-layer imports inside src/datacore/layers")
    parser.add_argument("root", nargs="?", default="src/datacore/layers", help="Root directory containing layer packages")
    parser.add_argument(
        "--allow",
        action="append",
        default=[],
        help="Explicit module prefixes allowed even if they cross layers",
    )
    args = parser.parse_args(argv)

    root = Path(args.root)
    violations = check_cross_layer(root, allowlist=args.allow)
    if violations:
        for violation in violations:
            print(violation.format(root), file=sys.stderr)
        print(
            "Cross-layer import guard failed. Each layer must remain isolated (raw → bronze → silver → gold).",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
