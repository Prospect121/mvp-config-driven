from __future__ import annotations

import importlib
import subprocess
from pathlib import Path
from typing import Any, Dict, List

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DummyEngine:
    def __init__(self) -> None:
        self.frames: List[Any] = []
        self.writes: List[Any] = []

    def createDataFrame(self, data: Any, schema: Any | None = None) -> Any:
        frame = list(data)
        self.frames.append((schema, frame))
        return frame

    def record_write(self, name: str, frame: Any) -> None:
        self.writes.append((name, frame))


def _make_layer_config(layer: str, engine: DummyEngine, base_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    def add_layer_tag(df: Any, _engine: Any, cfg: Dict[str, Any]) -> Any:
        return [dict(item, layer=cfg.get("layer")) for item in df]

    def dq_passthrough(df: Any, *_: Any) -> Any:
        return df

    def capture_writer(df: Any, eng: DummyEngine, target: Dict[str, Any]) -> None:
        eng.record_write(target.get("name", layer), df)

    return {
        "compute": {"kind": "stub", "engine": engine},
        "io": {
            "source": {"kind": "stub", "data": base_data},
            "sink": {"kind": "stub", "writer": capture_writer, "name": layer},
        },
        "transform": {"functions": [add_layer_tag], "layer": layer},
        "dq": {"checks": [dq_passthrough]},
    }


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_layer_run_returns_tagged_rows(layer: str) -> None:
    module = importlib.import_module(f"datacore.layers.{layer}.main")
    engine = DummyEngine()
    base_data = [{"id": 1}, {"id": 2}]
    cfg = _make_layer_config(layer, engine, base_data)

    result = module.run(cfg)

    expected = [dict(item, layer=layer) for item in base_data]
    assert result == expected
    assert engine.writes == [(layer, expected)]


def test_layers_do_not_import_each_other() -> None:
    result = subprocess.run(
        [
            "rg",
            "from datacore.layers\\.",
            "src/datacore/layers",
            "-n",
        ],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert result.returncode == 1, result.stdout + result.stderr

