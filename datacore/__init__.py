"""Datacore paquete multinube."""

__all__ = [
    "__version__",
    "build_plan",
    "run_pipeline",
    "validate_config_payload",
]

__version__ = "1.1.0"


def validate_config_payload(*args, **kwargs):
    from datacore.api import validate_config_payload as _validate_config_payload

    return _validate_config_payload(*args, **kwargs)


def build_plan(*args, **kwargs):
    from datacore.api import build_plan as _build_plan

    return _build_plan(*args, **kwargs)


def run_pipeline(*args, **kwargs):
    from datacore.api import run_pipeline as _run_pipeline

    return _run_pipeline(*args, **kwargs)
