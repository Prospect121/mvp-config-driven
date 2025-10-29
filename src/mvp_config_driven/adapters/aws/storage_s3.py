"""S3-backed storage adapter built on top of :mod:`s3fs`."""

from __future__ import annotations

from typing import Mapping, Tuple

import boto3
import s3fs
from botocore.exceptions import ClientError

from ...ports import StoragePort


class S3StorageAdapter(StoragePort):
    """Storage adapter that relies on AWS credentials resolved by ``boto3``."""

    def __init__(
        self,
        *,
        session: boto3.session.Session | None = None,
        s3fs_options: Mapping[str, object] | None = None,
    ) -> None:
        self._session = session or boto3.session.Session()
        options = dict(s3fs_options or {})
        options.setdefault("anon", False)

        credentials = self._session.get_credentials()
        if credentials is not None:
            frozen = credentials.get_frozen_credentials()
            if frozen.access_key and "key" not in options:
                options["key"] = frozen.access_key
            if frozen.secret_key and "secret" not in options:
                options["secret"] = frozen.secret_key
            if frozen.token and "token" not in options:
                options["token"] = frozen.token

        region_name = self._session.region_name
        client_kwargs = dict(options.get("client_kwargs", {}))
        if region_name and "region_name" not in client_kwargs:
            client_kwargs["region_name"] = region_name
        if client_kwargs:
            options["client_kwargs"] = client_kwargs

        self._filesystem = s3fs.S3FileSystem(**options)
        self._client = self._session.client("s3")

    def read_text(self, uri: str) -> str:
        bucket, key = self._split(uri)
        response = self._client.get_object(Bucket=bucket, Key=key)
        body = response.get("Body")
        data = body.read() if body else b""
        return data.decode("utf-8")

    def exists(self, uri: str) -> bool:
        bucket, key = self._split(uri)
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def write_text(self, uri: str, content: str) -> None:
        """Utility helper for tests to store textual content."""

        bucket, key = self._split(uri)
        self._client.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))

    @staticmethod
    def _split(uri: str) -> Tuple[str, str]:
        if not uri.startswith("s3://"):
            raise ValueError(f"Unsupported URI: {uri}")
        path = uri[5:]
        bucket, _, key = path.partition("/")
        return bucket, key


__all__ = ["S3StorageAdapter"]
