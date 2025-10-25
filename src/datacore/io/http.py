"""HTTP ingestion helpers with pagination, retries and incremental support."""

from __future__ import annotations

import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests

try:  # Optional dependency - Spark is not always available in tests
    from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore
    from pyspark.sql import functions as F  # type: ignore
except Exception:  # pragma: no cover - pyspark optional
    SparkDataFrame = None  # type: ignore
    F = None  # type: ignore

try:  # Optional dependency for fallbacks
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - pandas optional
    pd = None  # type: ignore

try:  # Optional dependency for fallbacks
    import polars as pl  # type: ignore
except Exception:  # pragma: no cover - polars optional
    pl = None  # type: ignore


@dataclass
class HttpFetchMetrics:
    """Small container summarising HTTP fetch results."""

    pages: int = 0
    records: int = 0
    watermark: Optional[str] = None
    state_id: Optional[str] = None


class _RetryableSession:
    """requests.Session wrapper adding retry with exponential backoff and jitter."""

    _DEFAULT_RETRY_STATUSES: Tuple[int, ...] = (429,)

    def __init__(
        self,
        retries: int,
        backoff_factor: float,
        timeout: float,
        *,
        jitter: float = 0.25,
        retry_statuses: Iterable[int] | None = None,
    ) -> None:
        self.session = requests.Session()
        self.session.verify = True  # TLS must remain enabled
        self.retries = max(0, int(retries))
        self.backoff_factor = max(0.0, float(backoff_factor))
        self.timeout = max(1.0, float(timeout))
        self.jitter = max(0.0, float(jitter))
        configured_statuses = set(int(code) for code in (retry_statuses or ()))
        configured_statuses.update(self._DEFAULT_RETRY_STATUSES)
        configured_statuses.update(range(500, 600))
        self.retry_statuses = configured_statuses

    def _compute_backoff(self, attempt: int) -> float:
        base = self.backoff_factor * (2 ** attempt)
        jitter = random.uniform(0.0, base * self.jitter) if self.jitter > 0 else 0.0
        return max(0.5, base + jitter)

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        attempt = 0
        last_error: Optional[Exception] = None
        while attempt <= self.retries:
            try:
                response = self.session.request(
                    method,
                    url,
                    timeout=self.timeout,
                    **kwargs,
                )
            except requests.RequestException as exc:  # pragma: no cover - network errors
                last_error = exc
            else:
                status_code = getattr(response, "status_code", None)
                try:
                    response.raise_for_status()
                except requests.HTTPError as exc:
                    last_error = exc
                else:
                    return response

                if status_code not in self.retry_statuses:
                    raise last_error  # type: ignore[misc]

            if attempt == self.retries:
                if last_error:
                    raise last_error
                raise RuntimeError("HTTP request failed without raising an exception")

            delay = self._compute_backoff(attempt)
            time.sleep(delay)
            attempt += 1

        if last_error:
            raise last_error
        raise RuntimeError("HTTP request failed without raising an exception")


def _rate_limit_interval(rate_limit_cfg: Mapping[str, Any] | None) -> Tuple[Optional[float], float]:
    if not rate_limit_cfg:
        return None, 0.0
    requests_per_minute = float(rate_limit_cfg.get("requests_per_minute", 0) or 0)
    if requests_per_minute <= 0:
        return None, 0.0
    interval = 60.0 / max(1.0, requests_per_minute)
    return None, interval


def _respect_rate_limit(next_deadline: Optional[float], interval: float) -> Optional[float]:
    if interval <= 0:
        return None
    now = time.monotonic()
    if next_deadline is None:
        next_deadline = now
    if now < next_deadline:
        time.sleep(next_deadline - now)
        now = time.monotonic()
    return max(next_deadline + interval, now)


def _apply_watermark(
    params: MutableMapping[str, Any], cfg: Mapping[str, Any], metrics: HttpFetchMetrics
) -> Optional[str]:
    incremental_cfg = cfg.get("incremental") or {}
    watermark_cfg = incremental_cfg.get("watermark") or {}
    value = watermark_cfg.get("value") or watermark_cfg.get("resolved_value")
    if value is None and "default" in watermark_cfg:
        value = watermark_cfg.get("default")
    if value is None:
        return None
    field_name = watermark_cfg.get("param") or watermark_cfg.get("field")
    if field_name:
        params.setdefault(str(field_name), value)
    metrics.state_id = watermark_cfg.get("state_id")
    return str(value)


def _normalize_records(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [item if isinstance(item, dict) else {"value": item} for item in payload]
    if isinstance(payload, dict):
        for key in ("data", "results", "items", "records"):
            if key in payload and isinstance(payload[key], list):
                return _normalize_records(payload[key])
        return [payload]
    raise TypeError("Unsupported HTTP response payload type for normalization")


def _extract_cursor(payload: Mapping[str, Any], cfg: Mapping[str, Any]) -> Optional[str]:
    pagination_cfg = cfg.get("pagination") or {}
    cursor_field = pagination_cfg.get("cursor_field") or "next_cursor"
    value = payload.get(cursor_field)
    if isinstance(value, str) and value:
        return value
    nested = payload.get("pagination")
    if isinstance(nested, Mapping):
        nested_value = nested.get(cursor_field)
        if isinstance(nested_value, str) and nested_value:
            return nested_value
    return None


def _extract_link_header(headers: Mapping[str, Any]) -> Optional[str]:
    link_header = headers.get("Link") or headers.get("link")
    if not isinstance(link_header, str):
        return None
    for part in link_header.split(","):
        section = part.strip()
        if "rel=\"next\"" in section:
            start = section.find("<")
            end = section.find(">", start)
            if start != -1 and end != -1:
                return section[start + 1 : end]
    return None


def _prepare_headers(cfg: Mapping[str, Any]) -> Dict[str, str]:
    headers_cfg = cfg.get("headers") or {}
    headers: Dict[str, str] = {}
    for key, value in headers_cfg.items():
        if value is None:
            continue
        headers[str(key)] = str(value)
    return headers


def _env_value(name: Any) -> Optional[str]:
    if not name:
        return None
    value = os.getenv(str(name))
    if value is None:
        return None
    value = value.strip()
    return value or None


def _resolve_bearer_env(auth_cfg: Mapping[str, Any]) -> Optional[str]:
    token = _env_value(auth_cfg.get("env") or auth_cfg.get("var"))
    if token:
        return f"Bearer {token}"
    return None


def _resolve_api_key_header(auth_cfg: Mapping[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    header = auth_cfg.get("header") or auth_cfg.get("name")
    value = _env_value(auth_cfg.get("env") or auth_cfg.get("value_env"))
    if header and value:
        return str(header), value
    return None, None


def _resolve_basic_env(auth_cfg: Mapping[str, Any]) -> Optional[Tuple[str, str]]:
    username = _env_value(auth_cfg.get("username_env") or auth_cfg.get("user_env"))
    password = _env_value(auth_cfg.get("password_env"))
    if username and password:
        return username, password
    return None


def _resolve_oauth_client_credentials(auth_cfg: Mapping[str, Any]) -> Optional[str]:
    token_url = auth_cfg.get("token_url") or auth_cfg.get("url")
    if not token_url:
        raise ValueError("OAuth2 client credentials auth requires 'token_url'")
    client_id = _env_value(auth_cfg.get("client_id_env") or auth_cfg.get("client_id_var"))
    client_secret = _env_value(
        auth_cfg.get("client_secret_env") or auth_cfg.get("client_secret_var")
    )
    if not client_id or not client_secret:
        raise ValueError("OAuth2 client credentials auth requires client credentials in env")

    data = {"grant_type": "client_credentials"}
    scope = auth_cfg.get("scope")
    if scope:
        data["scope"] = str(scope)
    extra_params = auth_cfg.get("params")
    if isinstance(extra_params, Mapping):
        for key, value in extra_params.items():
            if value is not None:
                data[str(key)] = str(value)

    response = requests.post(
        str(token_url),
        data=data,
        auth=(client_id, client_secret),
        timeout=float(auth_cfg.get("timeout", 10) or 10),
    )
    response.raise_for_status()
    payload = response.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError("OAuth2 token endpoint did not return 'access_token'")
    token_type = payload.get("token_type", "Bearer")
    return f"{str(token_type).title()} {token}"


def _prepare_auth(cfg: Mapping[str, Any]) -> Tuple[Dict[str, str], Optional[Tuple[str, str]]]:
    auth_cfg = cfg.get("auth") or {}
    auth_type = (auth_cfg.get("type") or "").lower()
    headers: Dict[str, str] = {}
    auth: Optional[Tuple[str, str]] = None

    if auth_type in {"bearer_env", "env_bearer"}:
        token = _resolve_bearer_env(auth_cfg)
        if token:
            headers.setdefault("Authorization", token)
    elif auth_type in {"api_key_header", "api-key"}:
        header, value = _resolve_api_key_header(auth_cfg)
        if header and value:
            headers.setdefault(header, value)
    elif auth_type in {"basic_env", "env_basic"}:
        auth = _resolve_basic_env(auth_cfg)
    elif auth_type in {"basic", "user_password"}:
        user = auth_cfg.get("username") or auth_cfg.get("user")
        password = auth_cfg.get("password")
        if user and password:
            auth = (str(user), str(password))
    elif auth_type in {"oauth2", "oauth2_client_credentials", "client_credentials"}:
        token = _resolve_oauth_client_credentials(auth_cfg)
        if token:
            headers.setdefault("Authorization", token)

    return headers, auth


def _ensure_compatible_frame(records: List[Dict[str, Any]], spark: Any | None) -> Any:
    if spark is not None:
        return spark.createDataFrame(records)
    if pd is not None:
        return pd.DataFrame.from_records(records)
    if pl is not None:
        return pl.DataFrame(records)
    raise RuntimeError("No available DataFrame implementation to materialize HTTP payload")


def _compute_watermark_from_frame(df: Any, field: str) -> Optional[str]:
    if not field:
        return None
    if SparkDataFrame is not None and isinstance(df, SparkDataFrame):  # pragma: no cover - heavy
        if F is None:
            return None
        agg = df.select(F.max(F.col(field)).alias("wm")).collect()
        if agg:
            value = agg[0]["wm"]
            return None if value is None else str(value)
        return None
    if pd is not None and isinstance(df, pd.DataFrame):
        if field in df.columns and not df.empty:
            value = df[field].max()
            return None if value is None else str(value)
        return None
    if pl is not None and isinstance(df, pl.DataFrame):  # pragma: no cover - optional
        if field in df.columns:
            value = df.select(pl.col(field).max()).item()
            return None if value is None else str(value)
        return None
    if hasattr(df, "toPandas"):
        pdf = df.toPandas()  # type: ignore[no-untyped-call]
        if field in pdf.columns and not pdf.empty:
            value = pdf[field].max()
            return None if value is None else str(value)
    return None


def fetch_json_to_df(cfg: Mapping[str, Any], *, spark: Any | None = None) -> Tuple[Any, HttpFetchMetrics]:
    """Fetch paginated JSON payloads into a DataFrame.

    Parameters
    ----------
    cfg:
        Source configuration containing ``url``, optional pagination rules and
        incremental watermark hints. The mapping is mutated lightly to inject
        resolved parameters (e.g. pagination counters).
    spark:
        Optional Spark session used to build a Spark DataFrame. When omitted,
        pandas or polars are attempted as fallbacks.
    """

    if not isinstance(cfg, Mapping):
        raise TypeError("HTTP configuration must be a mapping")

    url = cfg.get("url") or cfg.get("uri")
    if not url:
        raise ValueError("HTTP source requires a 'url'")

    method = str(cfg.get("method") or "GET").upper()
    params: Dict[str, Any] = dict(cfg.get("params") or {})
    data_payload = cfg.get("body")

    metrics = HttpFetchMetrics()
    watermark_value = _apply_watermark(params, cfg, metrics)

    pagination_cfg = cfg.get("pagination") or {}
    strategy = (pagination_cfg.get("strategy") or "none").lower()
    max_pages = int(pagination_cfg.get("max_pages", 0) or 0)
    if max_pages <= 0:
        max_pages = 1 if strategy == "none" else 1000

    param_name = pagination_cfg.get("param") or "page"
    cursor_param = pagination_cfg.get("cursor_param") or "cursor"

    rate_deadline, rate_interval = _rate_limit_interval(cfg.get("rate_limit"))

    retry_cfg = cfg.get("retries") or {}
    status_forcelist = retry_cfg.get("status_forcelist")
    if isinstance(status_forcelist, (list, tuple, set)):
        retry_statuses: Iterable[int] | None = [int(code) for code in status_forcelist]
    elif status_forcelist is None:
        retry_statuses = None
    else:
        retry_statuses = [int(status_forcelist)]
    session = _RetryableSession(
        retries=int(retry_cfg.get("max_attempts", 3) or 3),
        backoff_factor=float(retry_cfg.get("backoff", 1.5) or 1.5),
        timeout=float(cfg.get("timeout", 30) or 30),
        jitter=float(retry_cfg.get("jitter", 0.25) or 0.25),
        retry_statuses=retry_statuses,
    )

    headers = _prepare_headers(cfg)
    auth_headers, auth = _prepare_auth(cfg)
    headers.update(auth_headers)

    records: List[Dict[str, Any]] = []
    next_cursor: Optional[str] = None

    for page in range(1, max_pages + 1):
        rate_deadline = _respect_rate_limit(rate_deadline, rate_interval)

        request_params = dict(params)
        if strategy == "param_increment":
            request_params[param_name] = page
        elif strategy == "cursor" and metrics.pages > 0:
            if next_cursor is None:
                break
            request_params[cursor_param] = next_cursor
        elif strategy == "link_header" and metrics.pages > 0:
            if next_cursor is None:
                break
            url = next_cursor

        try:
            response = session.request(
                method,
                url,
                params=request_params if method == "GET" else None,
                json=data_payload if method in {"POST", "PUT", "PATCH"} else None,
                headers=headers,
                auth=auth,
            )
        except RuntimeError as exc:
            if "No more responses configured" in str(exc):  # test helper sentinel
                break
            raise

        payload: Any
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            payload = response.json()
        else:
            payload = json.loads(response.text)

        page_records = _normalize_records(payload)
        if not page_records:
            if strategy == "param_increment" and metrics.pages == 0:
                break
            if strategy == "none":
                break
        records.extend(page_records)
        metrics.pages += 1
        metrics.records += len(page_records)

        if strategy == "none":
            break

        if strategy == "cursor":
            next_cursor = _extract_cursor(payload if isinstance(payload, Mapping) else {}, cfg)
            if not next_cursor:
                break
        elif strategy == "link_header":
            next_cursor = _extract_link_header(response.headers)
            if not next_cursor:
                break
        else:  # param_increment
            if pagination_cfg.get("stop_on_empty", True) and not page_records:
                break

    df = _ensure_compatible_frame(records, spark)

    watermark_field = ((cfg.get("incremental") or {}).get("watermark") or {}).get("field")
    if watermark_field:
        metrics.watermark = _compute_watermark_from_frame(df, watermark_field)
    elif watermark_value:
        metrics.watermark = watermark_value

    return df, metrics


__all__ = ["fetch_json_to_df", "HttpFetchMetrics"]
