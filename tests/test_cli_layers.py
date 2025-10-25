from __future__ import annotations

from pathlib import Path

import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest

from datacore.cli import app

pytest.importorskip("typer.testing")
from typer.testing import CliRunner

LAYERS = ("raw", "bronze", "silver", "gold")


@pytest.mark.parametrize("layer", LAYERS)
def test_run_layer_examples_are_dry(layer: str) -> None:
    runner = CliRunner()
    config_path = Path("cfg") / layer / "example.yml"
    assert config_path.exists(), f"Missing example config for layer {layer}"
    result = runner.invoke(app, ["run-layer", layer, "-c", str(config_path)])
    assert result.exit_code == 0
    assert "Dry run requested" in result.stdout


def test_run_pipeline_example_config() -> None:
    runner = CliRunner()
    pipeline_cfg = Path("cfg/pipelines/example.yml")
    assert pipeline_cfg.exists(), "Missing example pipeline config"
    result = runner.invoke(app, ["run-pipeline", "-p", str(pipeline_cfg)])
    assert result.exit_code == 0
