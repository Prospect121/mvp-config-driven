import os
import json
import yaml
from typing import Any, Dict


def _load_yaml_or_json(path: str) -> Dict[str, Any]:
    """Load YAML or JSON file into dict, auto-detecting extension."""
    _, ext = os.path.splitext(path.lower())
    with open(path, 'r', encoding='utf-8') as f:
        if ext in ('.yml', '.yaml'):
            return yaml.safe_load(f) or {}
        try:
            return json.load(f)
        except Exception:
            f.seek(0)
            return yaml.safe_load(f) or {}


def load_dataset_config(path: str) -> Dict[str, Any]:
    return _load_yaml_or_json(path)


def load_env_config(path: str) -> Dict[str, Any]:
    return _load_yaml_or_json(path)


def load_db_config(path: str) -> Dict[str, Any]:
    return _load_yaml_or_json(path)