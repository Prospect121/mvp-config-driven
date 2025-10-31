import json
from pathlib import Path

from jsonschema import Draft7Validator

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
