from __future__ import annotations

import sys
from pathlib import Path

import pytest

typer_testing = pytest.importorskip("typer.testing")
CliRunner = typer_testing.CliRunner

from datacore.cli import app, main


@pytest.mark.parametrize("layer", ["raw", "bronze", "silver", "gold"])
def test_run_layer_dry_run(layer: str) -> None:
    runner = CliRunner()
    config_path = Path("cfg") / layer / "template.yml"
    result = runner.invoke(app, [layer, "-c", str(config_path)])
    assert result.exit_code == 0
    assert "Dry run requested" in result.stdout


def test_run_layer_alias(monkeypatch) -> None:
    config_path = Path("cfg/raw/template.yml")
    argv = ["prodi", "run-layer", "raw", "-c", str(config_path)]
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 0
