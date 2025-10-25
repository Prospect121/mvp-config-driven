"""Catalog helpers for persistent dataset metadata."""

from .state import (
    WatermarkState,
    load_dataset_state,
    read_watermark,
    save_dataset_state,
    update_watermark,
)

__all__ = [
    "WatermarkState",
    "load_dataset_state",
    "read_watermark",
    "save_dataset_state",
    "update_watermark",
]
