"""Utilidades de observabilidad para ejecuciones de datasets."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any

LOGGER = logging.getLogger(__name__)

try:  # pragma: no cover - dependencias opcionales
    from prometheus_client import Counter, Histogram  # type: ignore
except ImportError:  # pragma: no cover - entorno sin prometheus
    Counter = Histogram = None  # type: ignore

if Counter is not None:  # pragma: no cover - solo se ejecuta si está disponible
    DATASET_RUNS = Counter(
        "prodi_dataset_runs_total",
        "Total de ejecuciones de datasets",
        ["dataset", "status"],
    )
else:  # pragma: no cover - entorno sin prometheus
    DATASET_RUNS = None

if Histogram is not None:  # pragma: no cover - solo se ejecuta si está disponible
    DATASET_DURATION = Histogram(
        "prodi_dataset_duration_seconds",
        "Duración de ejecuciones de datasets",
        ["dataset"],
    )
else:  # pragma: no cover
    DATASET_DURATION = None


def new_run_id() -> str:
    """Genera un identificador único para la ejecución."""

    return str(uuid.uuid4())


def record_dataset(dataset: str, status: str, duration: float, metrics: dict[str, Any] | None) -> None:
    """Registra métricas de observabilidad de forma defensiva."""

    if DATASET_RUNS is not None:
        DATASET_RUNS.labels(dataset=dataset, status=status).inc()
    if DATASET_DURATION is not None:
        DATASET_DURATION.labels(dataset=dataset).observe(duration)
    LOGGER.debug(
        "Observabilidad dataset=%s status=%s duration=%.3fs métricas=%s",
        dataset,
        status,
        duration,
        sorted(metrics.keys()) if metrics else [],
    )


def emit_openlineage(payload: dict[str, Any], config: dict[str, Any]) -> None:
    """Emite eventos OpenLineage cuando la dependencia está disponible."""

    if not config or not config.get("enabled", True):
        return
    endpoint = config.get("url")
    if not endpoint:
        LOGGER.debug("OpenLineage habilitado sin URL, se omite")
        return
    try:  # pragma: no cover - librería opcional
        from openlineage.client import OpenLineageClient  # type: ignore
        from openlineage.client.run import Job, Run, RunEvent, RunState  # type: ignore
    except ImportError:  # pragma: no cover - librería no instalada
        LOGGER.debug("Cliente OpenLineage no disponible, se omite evento")
        return

    client = OpenLineageClient(url=endpoint, api_key=config.get("api_key"))
    namespace = config.get("namespace", "prodi")
    dataset_name = payload.get("dataset", "desconocido")
    run = Run(runId=payload.get("run_id"))
    job = Job(namespace=namespace, name=f"{payload.get('layer', 'unknown')}::{dataset_name}")
    status = payload.get("status", "COMPLETED").upper()
    try:
        state = RunState.from_string(status)
    except ValueError:
        state = RunState.COMPLETE
    event = RunEvent(
        eventType=state,
        eventTime=datetime.utcnow().isoformat() + "Z",
        run=run,
        job=job,
        inputs=[],
        outputs=[],
    )
    client.emit(event)
