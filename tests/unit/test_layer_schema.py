import json
from pathlib import Path

import pytest
from jsonschema import Draft7Validator, ValidationError

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "datacore" / "config" / "schemas" / "layer.schema.json"

with SCHEMA_PATH.open("r", encoding="utf-8") as handle:
    SCHEMA = json.load(handle)

VALIDATOR = Draft7Validator(SCHEMA)


def _base_config(dataset: dict) -> dict:
    return {
        "project": "demo",
        "environment": "dev",
        "platform": "azure",
        "datasets": [dataset],
    }


def test_schema_accepts_single_source_object():
    dataset = {
        "name": "customers",
        "layer": "silver",
        "source": {
            "type": "storage",
            "format": "parquet",
            "uri": "abfss://bronze@contoso.dfs.core.windows.net/customers/",
        },
        "sink": {
            "type": "storage",
            "format": "parquet",
            "uri": "abfss://silver@contoso.dfs.core.windows.net/customers/",
        },
    }

    VALIDATOR.validate(_base_config(dataset))


def test_schema_accepts_multi_source_list():
    dataset = {
        "name": "orders",
        "layer": "bronze",
        "source": [
            {"type": "storage", "format": "csv", "uri": "s3://raw/orders/"},
            {
                "type": "api_rest",
                "url": "https://api.example.com/v1/orders",
                "record_path": "items",
            },
        ],
        "sink": {
            "type": "warehouse",
            "engine": "synapse",
            "table": "analytics.orders_bronze",
            "mode": "append",
        },
    }

    VALIDATOR.validate(_base_config(dataset))


def test_schema_enforces_source_type_enum():
    dataset = {
        "name": "bad_dataset",
        "layer": "raw",
        "source": {"type": "unsupported"},
        "sink": {"type": "storage", "uri": "file:///tmp/out"},
    }

    with pytest.raises(ValidationError):
        VALIDATOR.validate(_base_config(dataset))


def test_schema_supports_sink_type_enum():
    dataset = {
        "name": "events",
        "layer": "bronze",
        "source": {"type": "kafka", "options": {"subscribe": "events"}},
        "sink": {"type": "nosql", "engine": "cosmosdb"},
    }

    VALIDATOR.validate(_base_config(dataset))
