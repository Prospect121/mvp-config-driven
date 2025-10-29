"""Glue job backend integrating with AWS Glue job invocations."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional

import boto3

from ...ports import JobBackendPort

try:  # pragma: no cover - pyspark remains optional in unit tests
    from pyspark.sql import SparkSession  # type: ignore
except Exception:  # pragma: no cover - running without Spark
    SparkSession = None  # type: ignore


def parse_awscli_arguments(values: Iterable[str]) -> dict[str, str]:
    """Parse AWS CLI style ``--key value`` arguments into a mapping."""

    parsed: dict[str, str] = {}
    iterator = iter(values)
    for token in iterator:
        if not token:
            continue
        if not token.startswith("--"):
            continue
        key = token[2:]
        try:
            value = next(iterator)
        except StopIteration:
            value = ""
        parsed[key] = value
    return parsed


class GlueJobBackend(JobBackendPort):
    """Job backend that wraps AWS Glue job helpers."""

    def __init__(
        self,
        *,
        session: boto3.session.Session | None = None,
        job_name: Optional[str] = None,
        default_arguments: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._session = session or boto3.session.Session()
        self._client = self._session.client("glue")
        self._job_name = job_name
        self._default_arguments = dict(default_arguments or {})

    @property
    def default_arguments(self) -> Mapping[str, str]:
        return dict(self._default_arguments)

    def ensure_engine(self, context: Any) -> Any:
        existing = getattr(context, "engine", None)
        if existing is not None:
            return existing
        if SparkSession is None:
            return None
        spark = SparkSession.builder.getOrCreate()
        context.engine = spark
        return spark

    def stop_engine(self, context: Any) -> None:
        engine = getattr(context, "engine", None)
        if engine is None:
            return
        try:
            engine.stop()
        finally:
            context.engine = None

    def start_job_run(self, *, arguments: Optional[Mapping[str, str]] = None) -> str:
        """Start a Glue job run using the resolved defaults."""

        if not self._job_name:
            raise RuntimeError("Glue job name is required to start a run")
        payload = dict(self._default_arguments)
        if arguments:
            payload.update({str(k): str(v) for k, v in arguments.items()})
        response = self._client.start_job_run(JobName=self._job_name, Arguments=payload)
        return str(response["JobRunId"])


__all__ = ["GlueJobBackend", "parse_awscli_arguments"]
