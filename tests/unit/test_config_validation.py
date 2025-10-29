import pytest

from datacore.config.validation import validate_config


def test_validate_project_schema(tmp_path):
    data = {
        "project": "demo",
        "environment": "dev",
        "platform": "aws",
        "datasets": [
            {
                "name": "sample",
                "layer": "raw",
                "source": {"type": "storage", "uri": "s3://bucket/path"},
                "sink": {"type": "storage", "uri": "s3://bucket/out"},
            }
        ],
    }
    validate_config(data)


def test_validate_invalid_schema():
    data = {"project": "demo"}
    with pytest.raises(ValueError):
        validate_config(data)
