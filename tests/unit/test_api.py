from datacore import api as datacore_api


def _sample_project() -> dict:
    return {
        "project": "demo",
        "environment": "dev",
        "platform": "local",
        "datasets": [
            {
                "name": "raw_ds",
                "layer": "raw",
                "source": {"type": "storage", "uri": "/tmp/in", "backend": "local"},
                "sink": {"type": "storage", "uri": "/tmp/out", "backend": "local"},
            },
            {
                "name": "silver_ds",
                "layer": "silver",
                "source": {"type": "storage", "uri": "/tmp/in2", "backend": "local"},
                "sink": {"type": "storage", "uri": "/tmp/out2", "backend": "local"},
            },
        ],
    }


def test_validate_config_payload_returns_schema(monkeypatch):
    monkeypatch.setattr(datacore_api, "validate_config", lambda payload, schema=None: None)
    result = datacore_api.validate_config_payload(_sample_project())
    assert result["valid"] is True
    assert result["schema"] == "project"


def test_build_plan_single_layer(monkeypatch):
    calls = []

    def fake_validate(payload, schema=None):
        return None

    def fake_run_layer_plan(**kwargs):
        calls.append(kwargs)
        return {"run_id": "run-1", "created_at": "2026-01-01T00:00:00Z", "datasets": []}

    monkeypatch.setattr(datacore_api, "validate_config", fake_validate)
    monkeypatch.setattr(datacore_api, "run_layer_plan", fake_run_layer_plan)

    result = datacore_api.build_plan(_sample_project(), layer="raw", platform="local", env="dev")
    assert result["run_id"] == "run-1"
    assert len(calls) == 1
    assert calls[0]["dry_run"] is True
    assert calls[0]["layer"] == "raw"


def test_build_plan_aggregates_layers(monkeypatch):
    calls = []

    def fake_validate(payload, schema=None):
        return None

    def fake_run_layer_plan(**kwargs):
        calls.append(kwargs["layer"])
        return {
            "run_id": f"run-{kwargs['layer']}",
            "created_at": "2026-01-01T00:00:00Z",
            "datasets": [{"name": f"{kwargs['layer']}-ds", "status": "planned"}],
        }

    monkeypatch.setattr(datacore_api, "validate_config", fake_validate)
    monkeypatch.setattr(datacore_api, "run_layer_plan", fake_run_layer_plan)

    result = datacore_api.build_plan(_sample_project(), platform="local")
    assert set(calls) == {"raw", "silver"}
    assert len(result["datasets"]) == 2


def test_run_pipeline_calls_engine(monkeypatch):
    calls = []

    def fake_validate(payload, schema=None):
        return None

    def fake_run_layer_plan(**kwargs):
        calls.append(kwargs)
        return {"run_id": "run-x", "datasets": [{"name": "raw_ds", "status": "completed"}]}

    monkeypatch.setattr(datacore_api, "validate_config", fake_validate)
    monkeypatch.setattr(datacore_api, "run_layer_plan", fake_run_layer_plan)

    result = datacore_api.run_pipeline(_sample_project(), layer="raw", platform="local", env="dev")
    assert result["run_id"] == "run-x"
    assert len(calls) == 1
    assert calls[0]["dry_run"] is False
