from __future__ import annotations

from pathlib import Path

import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest
import yaml

from datacore.cli import app

pytest.importorskip("typer.testing")
from typer.testing import CliRunner

DRY_LAYERS = ("bronze", "silver", "gold")


@pytest.mark.parametrize("layer", DRY_LAYERS)
def test_run_layer_examples_are_dry(layer: str) -> None:
    runner = CliRunner()
    config_path = Path("cfg") / layer / "example.yml"
    assert config_path.exists(), f"Missing example config for layer {layer}"
    result = runner.invoke(app, ["run-layer", layer, "-c", str(config_path)])
    assert result.exit_code == 0
    assert "Dry run requested" in result.stdout


def test_run_layer_raw_example_executes(tmp_path: Path) -> None:
    runner = CliRunner()
    config_path = Path("cfg/raw/example.yml")
    assert config_path.exists(), "Missing example config for Raw layer"

    config_data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    output_dir = tmp_path / "raw-output"
    config_data.setdefault("io", {}).setdefault("sink", {})["uri"] = output_dir.as_uri()

    base_dir = Path(__file__).resolve().parents[1]
    paths_cfg = config_data.setdefault("paths", {})
    for key in ("dataset", "environment", "database"):
        value = paths_cfg.get(key)
        if value:
            absolute = (base_dir / value).resolve()
            paths_cfg[key] = str(absolute)

    source_cfg = config_data.setdefault("io", {}).setdefault("source", {})
    for key in ("dataset_config", "environment_config"):
        value = source_cfg.get(key)
        if value:
            source_cfg[key] = str((base_dir / value).resolve())
    source_cfg["use_local_fallback"] = True

    temp_cfg = tmp_path / "raw-config.yml"
    temp_cfg.write_text(yaml.safe_dump(config_data), encoding="utf-8")

    result = runner.invoke(app, ["run-layer", "raw", "-c", str(temp_cfg)])
    assert result.exit_code == 0
    assert output_dir.exists()
    if output_dir.is_dir():
        assert any(output_dir.iterdir()), "Raw sink directory should contain data"
    else:
        assert output_dir.is_file(), "Raw sink file was not created"


def test_validate_command(tmp_path: Path) -> None:
    runner = CliRunner()
    cfg_path = tmp_path / "layer.yml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "layer": "raw",
                "dry_run": True,
                "io": {
                    "source": {
                        "type": "http",
                        "url": "https://api.example.com",
                        "pagination": {"strategy": "none"},
                        "incremental": {"watermark": {"field": "updated_at", "state_id": "demo"}},
                    },
                    "sink": {"type": "files", "uri": str((tmp_path / "out").as_posix())},
                },
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["validate", "-c", str(cfg_path)])

    assert result.exit_code == 0
    assert "is valid" in result.stdout
