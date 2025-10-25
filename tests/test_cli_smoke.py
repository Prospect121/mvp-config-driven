from __future__ import annotations

import sys
from pathlib import Path
from typing import List

import pytest
import yaml

typer_testing = pytest.importorskip("typer.testing")
CliRunner = typer_testing.CliRunner

from datacore import cli as datacore_cli
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


def test_run_layer_only_executes_requested(monkeypatch, tmp_path) -> None:
    runner = CliRunner()
    config_path = tmp_path / "bronze.yml"
    config_path.write_text(yaml.safe_dump({"layer": "bronze", "dry_run": False}), encoding="utf-8")

    calls: List[str] = []

    def _fail_raw(cfg):  # pragma: no cover - defensive
        raise AssertionError("raw layer should not be executed")

    def _bronze(cfg):
        calls.append(cfg["layer"])
        return "bronze"

    monkeypatch.setitem(datacore_cli._LAYER_RUNNERS, "raw", _fail_raw)
    monkeypatch.setitem(datacore_cli._LAYER_RUNNERS, "bronze", _bronze)

    result = runner.invoke(app, ["bronze", "-c", str(config_path)])

    assert result.exit_code == 0
    assert calls == ["bronze"]


def test_run_pipeline_uses_yaml_declaration(monkeypatch, tmp_path) -> None:
    runner = CliRunner()
    raw_cfg = tmp_path / "raw.yml"
    bronze_cfg = tmp_path / "bronze.yml"
    raw_cfg.write_text(yaml.safe_dump({"layer": "raw", "dry_run": False}), encoding="utf-8")
    bronze_cfg.write_text(yaml.safe_dump({"layer": "bronze", "dry_run": False}), encoding="utf-8")

    pipeline_cfg = tmp_path / "pipeline.yml"
    pipeline_cfg.write_text(
        yaml.safe_dump(
            [
                {"layer": "raw", "config": raw_cfg.name},
                {"layer": "bronze", "config": bronze_cfg.name},
            ]
        ),
        encoding="utf-8",
    )

    executed: List[str] = []

    def _record(layer: str):
        def _runner(cfg):
            executed.append(layer)
            return layer

        return _runner

    monkeypatch.setitem(datacore_cli._LAYER_RUNNERS, "raw", _record("raw"))
    monkeypatch.setitem(datacore_cli._LAYER_RUNNERS, "bronze", _record("bronze"))

    result = runner.invoke(app, ["run-pipeline", "-p", str(pipeline_cfg)])

    assert result.exit_code == 0
    assert executed == ["raw", "bronze"]
