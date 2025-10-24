from __future__ import annotations

import sys
from pathlib import Path

# Ensure legacy pipeline modules remain importable when running from packages.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PIPELINES_DIR = _PROJECT_ROOT / 'pipelines'
if str(_PIPELINES_DIR) not in sys.path:
    sys.path.append(str(_PIPELINES_DIR))
