import os
import json
import tempfile
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame, functions as F

# Usar import absoluto para el contexto de ejecución del script
from common import maybe_config_s3a


def _json_options(opts: Dict[str, Any]) -> Dict[str, Any]:
    """Default JSON options merged with user-provided ones."""
    defaults = {
        "multiline": False,
        "encoding": "UTF-8",
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": "_corrupt_record",
    }
    return {**defaults, **(opts or {})}


def _csv_options(opts: Dict[str, Any]) -> Dict[str, Any]:
    defaults = {
        "header": True,
        "inferSchema": False,
        "sep": ",",
        "encoding": "UTF-8",
        "mode": "PERMISSIVE",
    }
    return {**defaults, **(opts or {})}


def _parquet_options(opts: Dict[str, Any]) -> Dict[str, Any]:
    defaults = {}
    return {**defaults, **(opts or {})}


def load_source(cfg: Dict[str, Any], spark: SparkSession, env: Dict[str, Any]) -> DataFrame:
    """Load a dataset source according to configuration.

    Supports csv, json, jsonl, parquet, jdbc, and api (optional staging),
    applying sane defaults and S3A setup.
    """
    src = cfg["source"]
    fmt = src.get("input_format")
    path = src.get("path")

    # Configure S3A automatically if path uses s3a
    if path:
        maybe_config_s3a(spark, path, env)

    # Reader base
    if fmt in ("csv", "json", "jsonl", "parquet"):
        reader = spark.read
        if fmt == "csv":
            opts = _csv_options(src.get("options", {}))
            reader = reader.options(**opts)
            return reader.csv(path)
        elif fmt == "json":
            opts = _json_options(src.get("options", {}))
            reader = reader.options(**opts)
            return reader.json(path)
        elif fmt == "jsonl":
            # JSON lines: same reader.json but ensure multiline False
            opts = _json_options(src.get("options", {}))
            opts["multiline"] = False
            reader = reader.options(**opts)
            return reader.json(path)
        elif fmt == "parquet":
            opts = _parquet_options(src.get("options", {}))
            reader = reader.options(**opts)
            return reader.parquet(path)

    elif src.get("type") == "jdbc":
        jdbc = src.get("jdbc", {})
        reader = (
            spark.read.format("jdbc")
            .option("url", jdbc.get("url"))
            .option("dbtable", jdbc.get("table"))
            .option("user", jdbc.get("user"))
            .option("password", jdbc.get("password"))
        )
        for k, v in jdbc.items():
            if k not in {"url", "table", "user", "password"}:
                reader = reader.option(k, v)
        return reader.load()

    elif src.get("type") == "api":
        api = src.get("api", {})

        # Resolver headers desde env.yml u variables de entorno
        headers: Dict[str, Any] = {}
        headers_ref = api.get("headers_ref")
        if headers_ref:
            # Buscar clave en env.yml
            if headers_ref in env:
                headers = env.get(headers_ref) or {}
            else:
                # Buscar variables de entorno con prefijo
                # Ej: headers_ref = "api_headers_payments" -> env vars API_HEADERS_PAYMENTS_*
                prefix = headers_ref.upper() + "_"
                for k, v in os.environ.items():
                    if k.upper().startswith(prefix):
                        headers[k[len(prefix):]] = v
        # Mezclar con headers explícitos
        headers.update(api.get("headers", {}))

        method = (api.get("method") or "GET").upper()
        endpoint = api.get("endpoint")
        if not endpoint:
            raise ValueError("API source requires 'endpoint'")

        page_param = api.get("page_param", "page")
        page_start = int(api.get("page_start", 1))
        page_size_param = api.get("page_size_param", "page_size")
        page_size = int(api.get("page_size", 1000))
        max_pages = int(api.get("max_pages", 100))
        items_key = api.get("items_key", "items")
        query_params = dict(api.get("query_params", {}))

        # Fallback sin dependencia: intentar usar 'requests' y si no está, urllib
        try:
            import requests  # type: ignore
            use_requests = True
        except Exception:
            use_requests = False
            import urllib.request
            import urllib.parse

        # Staging opcional: escribir JSONL en S3A o FS local para lectura robusta
        staging = api.get("staging", {})
        staging_enabled = bool(staging.get("enabled", False))
        staging_path = staging.get("path")
        staging_format = (staging.get("format") or "jsonl").lower()
        if staging_enabled and staging_path:
            maybe_config_s3a(spark, staging_path, env)

        all_rows: List[Dict[str, Any]] = []
        page = page_start
        while page <= max_pages:
            params = dict(query_params)
            params[page_param] = page
            params[page_size_param] = page_size

            try:
                if use_requests:
                    if method == "GET":
                        r = requests.get(endpoint, headers=headers, params=params, timeout=30)
                    else:
                        r = requests.post(endpoint, headers=headers, json=params, timeout=30)
                    status = r.status_code
                    body = r.text
                else:
                    url = endpoint
                    if method == "GET":
                        url = endpoint + "?" + urllib.parse.urlencode(params)
                        req = urllib.request.Request(url, method="GET", headers=headers)
                        with urllib.request.urlopen(req, timeout=30) as resp:
                            status = resp.getcode()
                            body = resp.read().decode("utf-8")
                    else:
                        data = json.dumps(params).encode("utf-8")
                        req = urllib.request.Request(endpoint, data=data, method="POST", headers=headers)
                        with urllib.request.urlopen(req, timeout=30) as resp:
                            status = resp.getcode()
                            body = resp.read().decode("utf-8")

                if status != 200:
                    raise RuntimeError(f"API error {status} page={page}")

                payload = json.loads(body)
                # Detectar lista de items
                if isinstance(payload, list):
                    items = payload
                elif isinstance(payload, dict):
                    # Preferir clave indicada, si no, intentar 'data' o 'results'
                    if items_key in payload and isinstance(payload[items_key], list):
                        items = payload[items_key]
                    elif "data" in payload and isinstance(payload["data"], list):
                        items = payload["data"]
                    elif "results" in payload and isinstance(payload["results"], list):
                        items = payload["results"]
                    else:
                        # Último recurso: si el dict representa un solo objeto, envolverlo
                        items = [payload]
                else:
                    items = []

                if not items:
                    # Terminar si no hay más; útil para paginado basado en contenido
                    break

                if staging_enabled and staging_path:
                    # Escribir chunk como JSONL temporal y subir vía Hadoop FS
                    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".jsonl", encoding="utf-8") as tf:
                        for row in items:
                            tf.write(json.dumps(row, ensure_ascii=False) + "\n")
                        temp_local = tf.name

                    jconf = spark._jsc.hadoopConfiguration()
                    jfs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(jconf)
                    JPath = spark._jvm.org.apache.hadoop.fs.Path
                    # Usar subcarpeta por página
                    target = JPath(staging_path + f"/page={page}/chunk.jsonl")
                    jfs.copyFromLocalFile(True, True, JPath(temp_local), target)
                else:
                    # Acumular en memoria para crear DataFrame directo
                    all_rows.extend(items)

            except Exception as e:
                raise RuntimeError(f"API fetch failed on page {page}: {e}")

            page += 1

        # Leer desde staging o crear DataFrame
        if staging_enabled and staging_path:
            opts = _json_options(src.get("options", {}))
            reader = spark.read.options(**opts)
            if staging_format == "parquet":
                return reader.parquet(staging_path)
            else:
                # jsonl/json: reader.json maneja ambos
                return reader.json(staging_path)
        else:
            if not all_rows:
                # Sin filas, crear DF vacío
                return spark.createDataFrame([], schema=None)
            return spark.createDataFrame(all_rows)

    else:
        raise ValueError(f"Unsupported source type/format: {src}")


def flatten_json(df: DataFrame, paths: Dict[str, Any] = None) -> DataFrame:
    """Flatten common nested objects like metadata and geo_location, configurable via paths.

    - If `paths` provided, expect mapping of {nested_field: [subfields_to_extract]}
    - Otherwise, flatten standard fields if present.
    """
    out = df
    default_paths = {
        "metadata": [
            "user_agent",
            "ip_address",
            "session_id",
            "referrer",
            "campaign_id",
        ],
        "geo_location": [
            "country",
            "city",
            "latitude",
            "longitude",
        ],
    }
    paths = paths or default_paths
    for parent, fields in paths.items():
        if parent in out.columns:
            for f in fields:
                col_path = f"{parent}.{f}"
                alias = f"{parent}_{f}"
                out = out.withColumn(alias, F.col(col_path))
    return out


def sanitize_nulls(df: DataFrame, fills: Dict[str, Any] = None, drop_if_null: Dict[str, bool] = None) -> DataFrame:
    """Handle nulls by filling defaults and optional row drops when key fields are null."""
    out = df
    if fills:
        out = out.fillna(fills)
    if drop_if_null:
        for col, enabled in drop_if_null.items():
            if enabled and col in out.columns:
                out = out.filter(F.col(col).isNotNull())
    return out


def project_columns(df: DataFrame, keep: Any = None) -> DataFrame:
    """Project to a subset of columns if configured."""
    if not keep:
        return df
    cols = [c for c in keep if c in df.columns]
    return df.select(*cols)