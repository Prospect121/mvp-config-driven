"""Databricks job backend integrating with the Databricks CLI."""

from __future__ import annotations

import json
from typing import Any, Iterable, Mapping, MutableMapping, Optional, Sequence

from ...ports import JobBackendPort


def _as_str_sequence(values: Optional[Sequence[Any]]) -> list[str]:
    if not values:
        return []
    return [str(value) for value in values]


def _merge_payload(base: MutableMapping[str, Any], extra: Optional[Mapping[str, Any]]) -> None:
    if not extra:
        return
    for key, value in extra.items():
        base[key] = value


class DatabricksJobBackend(JobBackendPort):
    """Job backend that prepares Databricks CLI invocations."""

    def __init__(
        self,
        *,
        cli_path: str = "databricks",
        profile: str | None = None,
        host: str | None = None,
        token: str | None = None,
        default_job_id: str | None = None,
        default_pipeline_id: str | None = None,
    ) -> None:
        self._cli_path = cli_path
        self._profile = profile
        self._host = host
        self._token = token
        self._default_job_id = default_job_id
        self._default_pipeline_id = default_pipeline_id

    def ensure_engine(self, context: object) -> Any:  # pragma: no cover - remote execution
        return None

    def stop_engine(self, context: object) -> None:  # pragma: no cover - remote execution
        return None

    def _base_command(self) -> list[str]:
        command = [self._cli_path]
        if self._profile:
            command.extend(["--profile", self._profile])
        if self._host:
            command.extend(["--host", self._host])
        if self._token:
            command.extend(["--token", self._token])
        return command

    def build_run_now_command(
        self,
        *,
        job_id: str | None = None,
        job_name: str | None = None,
        parameters: Mapping[str, Any] | None = None,
        notebook_params: Mapping[str, Any] | None = None,
        python_params: Sequence[Any] | None = None,
        jar_params: Sequence[Any] | None = None,
        spark_submit_params: Sequence[Any] | None = None,
        extra_payload: Mapping[str, Any] | None = None,
    ) -> list[str]:
        """Return the CLI command to trigger ``databricks jobs run-now``."""

        payload: MutableMapping[str, Any] = {}
        resolved_job_id = job_id or self._default_job_id
        if resolved_job_id:
            payload["job_id"] = resolved_job_id
        if job_name:
            payload["job_name"] = job_name
        _merge_payload(payload, parameters)
        if notebook_params:
            payload["notebook_params"] = dict(notebook_params)
        if python_params:
            payload["python_params"] = _as_str_sequence(python_params)
        if jar_params:
            payload["jar_params"] = _as_str_sequence(jar_params)
        if spark_submit_params:
            payload["spark_submit_params"] = _as_str_sequence(spark_submit_params)
        _merge_payload(payload, extra_payload)

        if not payload:
            raise ValueError("At least a job_id, job_name or payload must be provided")

        command = self._base_command()
        command.extend(["jobs", "run-now"])
        command.extend(["--json", json.dumps(payload, sort_keys=True)])
        return command

    def build_pipeline_start_command(
        self,
        *,
        pipeline_id: str | None = None,
        pipeline_name: str | None = None,
        full_refresh: bool | None = None,
    ) -> list[str]:
        """Return the CLI command to trigger ``databricks pipelines start``."""

        resolved_pipeline_id = pipeline_id or self._default_pipeline_id
        if not resolved_pipeline_id and not pipeline_name:
            raise ValueError("Pipeline id or name is required")

        command = self._base_command()
        command.extend(["pipelines", "start"])
        if resolved_pipeline_id:
            command.extend(["--pipeline-id", resolved_pipeline_id])
        if pipeline_name:
            command.extend(["--pipeline-name", pipeline_name])
        if full_refresh:
            command.append("--full-refresh")
        return command


__all__ = ["DatabricksJobBackend"]
