from datacore.core.engine import run_layer_plan


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
    assert plan[0]["status"] == "planned"
    assert "incremental.merge requiere keys" in plan[0]["issues"][0]
