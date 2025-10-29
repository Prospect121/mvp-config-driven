from datacore.core.engine import run_layer_plan


def test_run_layer_plan_local(tmp_path):
    data_path = tmp_path / "customers.csv"
    data_path.write_text("id,name\n1,Ana\n2,Juan\n", encoding="utf-8")
    config = {
        "project": "demo",
        "environment": "dev",
        "platform": "local",
        "datasets": [
            {
                "name": "customers_raw",
                "layer": "raw",
                "source": {
                    "type": "storage",
                    "format": "csv",
                    "uri": str(data_path),
                    "options": {"header": "true", "inferSchema": "true"},
                    "backend": "local",
                },
                "sink": {
                    "type": "storage",
                    "format": "parquet",
                    "uri": str(tmp_path / "raw"),
                    "backend": "local",
                },
            }
        ],
    }
    results = run_layer_plan("raw", config, platform_name="local", environment="dev")
    assert results[0]["status"] == "completed"
