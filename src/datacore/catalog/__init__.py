"""Catalog helpers for persistent dataset metadata."""

from .state import WatermarkState, get_watermark, load_state, save_state, set_watermark

__all__ = [
    "WatermarkState",
    "load_state",
    "get_watermark",
    "save_state",
    "set_watermark",
]
