"""Dataproc job backend that wraps ``gcloud dataproc batches submit`` invocations."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from ...ports import JobBackendPort, SecretsPort


class DataprocJobBackend(JobBackendPort):
    """Job backend that prepares ``gcloud dataproc`` batch commands."""

    def __init__(
        self,
        *,
        cli_path: str = "gcloud",
        component: str = "dataproc",
        default_project: str | None = None,
        default_region: str | None = None,
        default_service_account: str | None = None,
        default_subnet: str | None = None,
        default_batch_id: str | None = None,
        workload_identity: bool = True,
        secret_provider: SecretsPort | None = None,
        credentials_secret: str | None = None,
    ) -> None:
        self._cli_path = cli_path
        self._component = component
        self._default_project = default_project
        self._default_region = default_region
        self._default_service_account = default_service_account
        self._default_subnet = default_subnet
        self._default_batch_id = default_batch_id
        self._workload_identity = workload_identity
        self._secret_provider = secret_provider
        self._credentials_secret = credentials_secret
        self._cached_credentials_path: Path | None = None

    def ensure_engine(self, context: object) -> Any:  # pragma: no cover - remote execution
        return None

    def stop_engine(self, context: object) -> None:  # pragma: no cover - remote execution
        return None

    def build_batches_submit_command(
        self,
        *,
        batch_id: str | None = None,
        main_python_file_uri: str,
        project: str | None = None,
        region: str | None = None,
        service_account: str | None = None,
        subnet: str | None = None,
        container_image: str | None = None,
        jars: Sequence[str] | None = None,
        python_files: Sequence[str] | None = None,
        files: Sequence[str] | None = None,
        properties: Mapping[str, Any] | None = None,
        labels: Mapping[str, Any] | None = None,
        args: Sequence[Any] | None = None,
        extra_flags: Sequence[str] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> tuple[list[str], dict[str, str]]:
        """Return the command and environment variables for a Dataproc batch run."""

        resolved_batch = batch_id or self._default_batch_id
        if not resolved_batch:
            raise ValueError("batch_id is required to submit a Dataproc batch")

        resolved_project = project or self._default_project
        resolved_region = region or self._default_region
        resolved_service_account = service_account or self._default_service_account
        resolved_subnet = subnet or self._default_subnet

        command: list[str] = [
            self._cli_path,
            self._component,
            "batches",
            "submit",
            "pyspark",
            main_python_file_uri,
        ]
        command.extend(["--batch", resolved_batch])
        if resolved_region:
            command.extend(["--region", resolved_region])
        if resolved_project:
            command.extend(["--project", resolved_project])
        if resolved_service_account:
            command.extend(["--service-account", resolved_service_account])
        if resolved_subnet:
            command.extend(["--subnet", resolved_subnet])
        if container_image:
            command.extend(["--container-image", container_image])

        for jar in jars or []:
            command.extend(["--jar", str(jar)])
        for py_file in python_files or []:
            command.extend(["--py-file", str(py_file)])
        for file_uri in files or []:
            command.extend(["--file", str(file_uri)])

        if properties:
            serialized = self._serialize_mapping(properties)
            if serialized:
                command.extend(["--properties", serialized])
        if labels:
            serialized = self._serialize_mapping(labels)
            if serialized:
                command.extend(["--labels", serialized])

        if extra_flags:
            command.extend([str(flag) for flag in extra_flags])

        payload_args = [str(argument) for argument in args or []]
        if payload_args:
            command.append("--")
            command.extend(payload_args)

        resolved_env: dict[str, str] = dict(env or {})
        credentials_path = self._resolve_credentials_path()
        if credentials_path:
            resolved_env.setdefault("GOOGLE_APPLICATION_CREDENTIALS", credentials_path)

        return command, resolved_env

    @staticmethod
    def _serialize_mapping(values: Mapping[str, Any]) -> str:
        pairs = [f"{key}={values[key]}" for key in sorted(values)]
        return ",".join(pairs)

    def _resolve_credentials_path(self) -> str | None:
        if self._workload_identity:
            return None
        if not self._credentials_secret or self._secret_provider is None:
            return None
        if self._cached_credentials_path and self._cached_credentials_path.exists():
            return str(self._cached_credentials_path)

        payload = self._secret_provider.get_secret(self._credentials_secret)
        if not payload:
            return None

        try:
            json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            return None

        fd, raw_path = tempfile.mkstemp(prefix="dataproc-credentials-", suffix=".json")
        os.close(fd)
        path = Path(raw_path)
        path.write_text(str(payload), encoding="utf-8")
        self._cached_credentials_path = path
        return str(path)


__all__ = ["DataprocJobBackend"]
