"""Secrets adapter backed by AWS Secrets Manager."""

from __future__ import annotations

import base64
import json
from typing import Any, Dict, Mapping, Optional

import boto3

from ...ports import SecretsPort


class SecretsManagerAdapter(SecretsPort):
    """Resolve secrets from AWS Secrets Manager using ``boto3``."""

    def __init__(
        self,
        *,
        session: boto3.session.Session | None = None,
        cache_enabled: bool = True,
    ) -> None:
        self._session = session or boto3.session.Session()
        self._client = self._session.client("secretsmanager")
        self._cache_enabled = cache_enabled
        self._cache: Dict[str, Optional[str]] = {}

    def get_secret(self, key: str) -> Optional[str]:
        if self._cache_enabled and key in self._cache:
            return self._cache[key]

        try:
            response = self._client.get_secret_value(SecretId=key)
        except self._client.exceptions.ResourceNotFoundException:
            value: Optional[str] = None
        else:
            value = self._extract_secret_value(response)

        if self._cache_enabled:
            self._cache[key] = value
        return value

    def get_secret_dict(self, key: str) -> Mapping[str, Any]:
        secret = self.get_secret(key)
        if not secret:
            return {}
        try:
            data = json.loads(secret)
        except json.JSONDecodeError:
            return {}
        if not isinstance(data, Mapping):
            return {}
        return data

    @staticmethod
    def _extract_secret_value(response: Mapping[str, Any]) -> Optional[str]:
        secret = response.get("SecretString")
        if secret is not None:
            return str(secret)
        binary = response.get("SecretBinary")
        if binary is None:
            return None
        decoded = base64.b64decode(binary)
        return decoded.decode("utf-8")


__all__ = ["SecretsManagerAdapter"]
