from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable

import yaml

from datacore.config.schema import (
    DatasetConfigModel,
    LayerRuntimeConfigModel,
    migrate_dataset_config,
    migrate_layer_config,
)


def _load_yaml(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise TypeError(f"Configuration at {path} must be a mapping")
    return raw


def _iter_layer_config_files() -> Iterable[Path]:
    for path in sorted(Path("cfg").rglob("*.yml")):
        if "schemas" in path.parts:
            continue
        if "platforms" in path.parts:
            continue
        if path.name == "defaults.yml":
            continue
        yield path


def _iter_dataset_config_files() -> Iterable[Path]:
    for path in sorted(Path("config/datasets").rglob("*.yml")):
        if path.name.endswith("expectations.yml"):
            continue
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and (
            "id" in data or "layers" in data or "source" in data or "sources" in data or "output" in data
        ):
            yield path


def test_layer_configs_match_schema():
    for path in _iter_layer_config_files():
        raw_cfg = _load_yaml(path)
        migrated = migrate_layer_config(raw_cfg)
        model = LayerRuntimeConfigModel.model_validate(migrated)
        payload = model.model_dump(by_alias=True)
        assert payload["compute"]["engine"] == "spark"


def test_dataset_configs_match_schema_and_aliases():
    checked_any = False
    for path in _iter_dataset_config_files():
        raw_cfg = _load_yaml(path)
        migrated = migrate_dataset_config(raw_cfg)
        model = DatasetConfigModel.model_validate(migrated)
        payload = model.model_dump(by_alias=True)
        aliases = payload.get("_legacy_aliases", {})

        if "standardization" in raw_cfg:
            assert (
                aliases.get("standardization") == "layers.silver.transform.standardization"
            ), f"Missing standardization alias for {path}"
        if "quality" in raw_cfg:
            assert (
                aliases.get("quality") == "layers.silver.dq.expectations"
            ), f"Missing quality alias for {path}"

        checked_any = True

    assert checked_any, "No dataset configurations were discovered for validation"

