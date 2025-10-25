"""Lightweight persistence for dataset state such as incremental watermarks."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

_DEFAULT_STATE_DIR = Path("data/_state")


@dataclass
class WatermarkState:
    """Represents watermark information for a dataset source."""

    value: Optional[str] = None
    column: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"value": self.value, "field": self.column}
        if self.extra:
            payload["extra"] = dict(self.extra)
        return payload

    @classmethod
    def from_dict(cls, data: Dict[str, Any] | None) -> "WatermarkState":
        if not data:
            return cls()
        extra = data.get("extra") if isinstance(data.get("extra"), dict) else {}
        return cls(value=data.get("value"), column=data.get("field"), extra=dict(extra))


def _dataset_state_path(dataset: str, state_dir: Path | None = None) -> Path:
    target_dir = state_dir or _DEFAULT_STATE_DIR
    safe_dataset = dataset.replace("/", "_")
    return target_dir / f"{safe_dataset}.json"


def load_dataset_state(dataset: str, *, state_dir: Path | None = None) -> Dict[str, Any]:
    path = _dataset_state_path(dataset, state_dir)
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle) or {}
    except json.JSONDecodeError:
        # Corrupt state files should not break the pipeline; start fresh instead.
        return {}


def save_dataset_state(dataset: str, state: Dict[str, Any], *, state_dir: Path | None = None) -> None:
    path = _dataset_state_path(dataset, state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)


def read_watermark(
    dataset: str,
    key: str,
    *,
    state_dir: Path | None = None,
) -> WatermarkState:
    state = load_dataset_state(dataset, state_dir=state_dir)
    entry = state.get(key)
    if isinstance(entry, dict):
        return WatermarkState.from_dict(entry)
    if isinstance(entry, str):
        return WatermarkState(value=entry)
    return WatermarkState()


def update_watermark(
    dataset: str,
    key: str,
    value: Optional[str],
    *,
    field: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    state_dir: Path | None = None,
) -> None:
    if value is None:
        return
    payload = WatermarkState(value=value, column=field, extra=dict(extra or {})).to_dict()
    state = load_dataset_state(dataset, state_dir=state_dir)
    state[key] = payload
    save_dataset_state(dataset, state, state_dir=state_dir)


__all__ = [
    "WatermarkState",
    "load_dataset_state",
    "read_watermark",
    "save_dataset_state",
    "update_watermark",
]
