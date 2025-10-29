"""Lightweight persistence for dataset state such as incremental watermarks."""

from __future__ import annotations

import json

from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

_DEFAULT_STATE_DIR = Path("data/_state")


def _normalise_state_id(state_id: str) -> str:
    return state_id.replace("/", "_").replace("::", "_")


def _state_path(state_id: str, state_dir: Optional[Path] = None) -> Path:
    directory = state_dir or _DEFAULT_STATE_DIR
    return directory / f"{_normalise_state_id(state_id)}.json"


@dataclass
class WatermarkState:
    """Represents persisted information about a watermark."""

    value: Optional[str] = None
    field: Optional[str] = None
    updated_at: Optional[str] = None
    extra: Dict[str, Any] = dataclass_field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if self.value is not None:
            payload["value"] = self.value
        if self.field is not None:
            payload["field"] = self.field
        if self.updated_at is not None:
            payload["updated_at"] = self.updated_at
        if self.extra:
            payload["extra"] = dict(self.extra)
        return payload

    @classmethod
    def from_dict(cls, payload: Optional[Dict[str, Any]]) -> "WatermarkState":
        if not payload:
            return cls()
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
        return cls(
            value=payload.get("value"),
            field=payload.get("field"),
            updated_at=payload.get("updated_at"),
            extra=dict(extra),
        )


def load_state(state_id: str, *, state_dir: Optional[Path] = None) -> Dict[str, Any]:
    path = _state_path(state_id, state_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except json.JSONDecodeError:
        return {}


def save_state(state_id: str, state: Dict[str, Any], *, state_dir: Optional[Path] = None) -> None:
    path = _state_path(state_id, state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def get_watermark(state_id: str, *, state_dir: Optional[Path] = None) -> WatermarkState:
    state = load_state(state_id, state_dir=state_dir)
    return WatermarkState.from_dict(state)


def set_watermark(
    state_id: str,
    value: Optional[str],
    *,
    field: Optional[str] = None,
    state_dir: Optional[Path] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    if value is None:
        return
    payload = WatermarkState(
        value=value,
        field=field,
        updated_at=datetime.now(timezone.utc).isoformat(),
        extra=dict(extra or {}),
    ).to_dict()
    save_state(state_id, payload, state_dir=state_dir)


__all__ = ["WatermarkState", "get_watermark", "set_watermark", "load_state", "save_state"]
