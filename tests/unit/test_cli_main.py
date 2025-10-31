import json
from pathlib import Path

import pytest

from datacore.cli import main as cli_main


@pytest.fixture
def sample_config(tmp_path: Path) -> Path:
    config = tmp_path / "config.yaml"
    config.write_text(
        """
project: demo
environment: dev
platform: local
datasets:
  - name: demo
    layer: bronze
    source:
      type: storage
      uri: file:///tmp/input
    sink:
      type: storage
      uri: file:///tmp/output
"""
    )
    return config


def test_cli_validate(monkeypatch, sample_config):
    called = {}

    def fake_validate(data, schema=None):
        called["validated"] = (data, schema)

    monkeypatch.setattr(cli_main, "validate_config", fake_validate)
    cli_main.main(["validate", "--config", str(sample_config)])
    assert "validated" in called


def test_cli_run_dry_run(monkeypatch, sample_config, capsys):
    monkeypatch.setattr(cli_main, "validate_config", lambda data, schema=None: None)
    monkeypatch.setattr(
        cli_main,
        "run_layer_plan",
        lambda **kwargs: {"run_id": "run-123", "datasets": [{"name": "demo", "status": "planned"}]},
    )

    cli_main.main(
        [
            "run",
            "--layer",
            "bronze",
            "--config",
            str(sample_config),
            "--dry-run",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert output["run_id"] == "run-123"
    assert "layer" not in output
    assert output["datasets"][0]["status"] == "planned"


def test_cli_plan(monkeypatch, sample_config, capsys):
    monkeypatch.setattr(cli_main, "validate_config", lambda data, schema=None: None)

    def fake_plan(**kwargs):
        return {
            "run_id": f"run-{kwargs['layer']}",
            "datasets": [{"name": f"{kwargs['layer']}_ds", "status": "planned"}],
        }

    monkeypatch.setattr(cli_main, "run_layer_plan", fake_plan)

    cli_main.main(["plan", "--config", str(sample_config)])
    payload = json.loads(capsys.readouterr().out)
    assert payload["layers"][0]["run_id"].startswith("run-")
    assert payload["layers"][0]["datasets"][0]["status"] == "planned"
