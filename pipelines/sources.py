import os
import json
import tempfile
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame, functions as F

# Usar import absoluto para el contexto de ejecución del script
from pipelines.common import maybe_config_s3a


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


def _normalize_value_for_spark(value: Any) -> Any:
    """Normalize values to be Spark-friendly (e.g., numeric types)."""
    if isinstance(value, dict):
        return {k: _normalize_value_for_spark(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_value_for_spark(v) for v in value]
    # Convert Python bool to int? Keep as-is; Spark handles booleans.
    return value


def _normalize_items_for_spark(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize a list of dict rows to ensure consistent numeric types.

    This prevents PySpark's schema merger from failing with DoubleType vs LongType.
    """
    out: List[Dict[str, Any]] = []
    for row in items:
        if isinstance(row, dict):
            out.append({k: _normalize_value_for_spark(v) for k, v in row.items()})
        else:
            # Fallback for non-dict entries
            out.append(_normalize_value_for_spark(row))  # type: ignore
    return out


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
                    # Escribir directamente cada página a staging usando el writer de Spark
                    norm_items = _normalize_items_for_spark(items)
                    df_page = spark.createDataFrame(norm_items)
                    writer = df_page.write.mode("overwrite")
                    page_out = staging_path + f"/page={page}"
                    if staging_format == "parquet":
                        writer.parquet(page_out)
                    else:
                        # json/jsonl: ambos se leen con reader.json
                        writer.json(page_out)
                else:
                    # Acumular en memoria para crear DataFrame directo
                    all_rows.extend(_normalize_items_for_spark(items))

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


# ---- NUEVO: Soporte para múltiples fuentes ----

def load_source_any(src_cfg: Dict[str, Any], spark: SparkSession, env: Dict[str, Any]) -> DataFrame:
    """Load a single source definition (same structure as cfg['source'])."""
    tmp_cfg = {"source": src_cfg}
    return load_source(tmp_cfg, spark, env)


def load_sources_or_source(cfg: Dict[str, Any], spark: SparkSession, env: Dict[str, Any]) -> DataFrame:
    """Load from either a single source or a list of sources and union them.

    - If cfg has 'sources': iterate and union with allowMissingColumns
    - Else, fallback to existing 'source' behavior
    """
    sources_list = cfg.get("sources")
    if not sources_list:
        return load_source(cfg, spark, env)

    dfs: List[DataFrame] = []
    for idx, src in enumerate(sources_list):
        try:
            df_i = load_source_any(src, spark, env)
            dfs.append(df_i)
            path_or_table = src.get("path") or src.get("jdbc", {}).get("table") or src.get("api", {}).get("endpoint") or "<unknown>"
            print(f"[source] Loaded source[{idx}] rows={df_i.count()} from {path_or_table}")
        except Exception as e:
            print(f"[source] Warning: failed to load source[{idx}]: {e}")

    if not dfs:
        print("[source] Warning: no sources loaded; returning empty DataFrame")
        return spark.createDataFrame([], schema=None)

    # Union by name with missing columns allowed
    base = dfs[0]
    for other in dfs[1:]:
        try:
            base = base.unionByName(other, allowMissingColumns=True)
        except Exception as e:
            print(f"[source] Warning: union failed, attempting schema alignment: {e}")
            # Fallback: align columns manually by adding missing columns as nulls
            base_cols = set(base.columns)
            other_cols = set(other.columns)
            for c in base_cols - other_cols:
                other = other.withColumn(c, F.lit(None))
            for c in other_cols - base_cols:
                base = base.withColumn(c, F.lit(None))
            base = base.select(sorted(base.columns)).unionByName(other.select(sorted(other.columns)), allowMissingColumns=True)

    print(f"[source] Combined rows from {len(dfs)} sources: {base.count()}")
    return base


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


def flatten_json(df: DataFrame, paths: Dict[str, Any] = None) -> DataFrame:
    """Flatten nested JSON/struct columns into top-level fields.

    If `paths` is provided, it should be a mapping like:
    { "metadata": ["user_agent", "ip_address", ...], "geo_location": ["country", ...] }
    Otherwise, defaults will flatten common fields if present.
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