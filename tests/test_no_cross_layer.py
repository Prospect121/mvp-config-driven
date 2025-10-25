from __future__ import annotations

from pathlib import Path

from tools.check_cross_layer import check_cross_layer


def test_layers_do_not_import_siblings() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    layers_root = repo_root / "src" / "datacore" / "layers"
    violations = check_cross_layer(layers_root)
    assert not violations, "Cross-layer imports detected: " + ", ".join(
        violation.format(layers_root) for violation in violations
    )
