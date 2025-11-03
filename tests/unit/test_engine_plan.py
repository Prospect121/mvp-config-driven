import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator

from datacore.core import engine as engine_module
from datacore.core.engine import run_layer_plan

PLAN_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "datacore" / "config" / "schemas" / "plan.schema.json"

with PLAN_SCHEMA_PATH.open("r", encoding="utf-8") as handle:
    PLAN_SCHEMA = json.load(handle)

PLAN_VALIDATOR = Draft7Validator(PLAN_SCHEMA)


def test_run_layer_plan_dry_run(tmp_path):
    config = {
        "project": "demo",
        "environment": "dev",
        "platform": "local",
        "datasets": [
            {
                "name": "customers_raw",
                "layer": "bronze",
                "source": {
                    "type": "storage",
                    "uri": str(tmp_path / "in"),
                    "backend": "local",
                },
                "sink": {
                    "type": "storage",
                    "uri": str(tmp_path / "out"),
                    "backend": "local",
                },
                "incremental": {"mode": "merge"},
            }
        ],
    }

    plan = run_layer_plan("bronze", config, dry_run=True)
    assert "run_id" in plan
    PLAN_VALIDATOR.validate(plan)
    dataset_plan = plan["datasets"][0]
    assert dataset_plan["status"] == "planned"
    assert "source_plan" in dataset_plan
    assert any("incremental.merge requiere keys" in issue for issue in dataset_plan["issues"])


def test_plan_streaming_contract(tmp_path):
    config = {
        "project": "demo",
        "environment": "dev",
        "platform": "local",
        "datasets": [
            {
                "name": "orders_stream",
                "layer": "bronze",
                "streaming": {
                    "enabled": True,
                    "trigger": "10 minutes",
                    "checkpoint_location": str(tmp_path / "chk"),
                },
                "incremental": {
                    "mode": "append",
                    "watermark": {"column": "created_at", "delay_threshold": "5 minutes"},
                },
                "source": {
                    "type": "kafka",
                    "options": {"subscribe": "orders", "kafka.bootstrap.servers": "localhost:9092"},
                    "payload_format": "json",
                },
                "sink": {"type": "nosql", "engine": "cosmosdb", "options": {}},
            }
        ],
    }

    plan = run_layer_plan("bronze", config, dry_run=True)
    PLAN_VALIDATOR.validate(plan)
    dataset_plan = plan["datasets"][0]
    streaming_plan = dataset_plan["streaming_plan"]
    assert streaming_plan["enabled"] is True
    assert streaming_plan["trigger"] == "10 minutes"
    assert streaming_plan["checkpoint_location"] == str(tmp_path / "chk")
    assert streaming_plan["watermark"]["column"] == "created_at"


@pytest.mark.parametrize(
    "config_rel_path, layer, dataset_name",
    [
        ("examples/streaming/orders_stream.yaml", "bronze", "orders_stream"),
        ("examples/streaming/kafka_cosmos.yaml", "bronze", "orders_stream"),
    ],
)
def test_streaming_examples_emit_watermark_and_checkpoint(monkeypatch, config_rel_path, layer, dataset_name):
    base_dir = Path(__file__).resolve().parents[2]
    config = yaml.safe_load((base_dir / config_rel_path).read_text(encoding="utf-8"))
    config = dict(config)
    config["platform"] = "local"
    monkeypatch.setattr(engine_module, "_prepare_spark", lambda platform, conf: object())
    plan = engine_module.run_layer_plan(layer, config, platform_name="local", dry_run=True)
    PLAN_VALIDATOR.validate(plan)
    dataset_plan = next(ds for ds in plan["datasets"] if ds["name"] == dataset_name)
    streaming_plan = dataset_plan["streaming_plan"]
    assert streaming_plan["checkpoint_location"]
    watermark = streaming_plan.get("watermark")
    assert watermark
    assert watermark.get("column")
    assert watermark.get("delay_threshold")
