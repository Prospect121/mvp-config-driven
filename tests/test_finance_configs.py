from pathlib import Path

import yaml

CONFIG_ROOT = Path("cfg/envs/finance-prod-aws")


def _load_yaml(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_finance_raw_http_config_structure():
    cfg = _load_yaml(CONFIG_ROOT / "raw.yml")
    assert cfg["layer"] == "raw"
    source = cfg["storage"]["source"]
    assert source["type"] == "http"
    assert source["pagination"]["strategy"] == "param_increment"
    watermark = cfg["incremental"]["watermark"]
    assert watermark["field"] == "updated_at"


def test_finance_bronze_config_sql_contains_dedup():
    cfg = _load_yaml(CONFIG_ROOT / "bronze.yml")
    sql = cfg["transformations"]["sql"]
    assert "ROW_NUMBER() OVER" in sql
    assert "FROM __INPUT__" in sql


def test_finance_gold_config_sql_contains_metrics():
    cfg = _load_yaml(CONFIG_ROOT / "gold.yml")
    sql = cfg["transformations"]["sql"]
    assert "SUM(amount)" in sql
    assert "GROUP BY posted_date" in sql
