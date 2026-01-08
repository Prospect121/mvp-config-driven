"""Escritores genéricos y especializados."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

try:  # pragma: no cover - dependencia opcional
    import boto3
except ImportError:  # pragma: no cover - entornos sin boto3
    boto3 = None  # type: ignore[assignment]

from pyspark.sql import DataFrame

from datacore.connectors.db import jdbc
from datacore.connectors.storage import abfs, fabric, gcs, local, s3
from datacore.platforms.base import PlatformBase
from datacore.utils.logging import get_logger

LOGGER = get_logger(__name__)


def _storage_connector(backend: str):
    connectors = {
        "aws": s3,
        "azure": abfs,
        "gcp": gcs,
        "fabric": fabric,
        "local": local,
    }
    return connectors.get(backend, local)


def _merge_schema_flag(sink: dict[str, Any]) -> bool | None:
    if "mergeSchema" in sink:
        return bool(sink["mergeSchema"])
    if "merge_schema" in sink:
        return bool(sink["merge_schema"])
    return None


def _merge_write_options(sink: dict[str, Any]) -> dict[str, Any]:
    options = {**sink.get("options", {})}
    options.update(sink.get("write_options", {}))
    if "compression" in sink and "compression" not in options:
        options["compression"] = sink["compression"]
    if sink.get("atomic") is not None and "atomic" not in options:
        options["atomic"] = sink["atomic"]
    return options


def _apply_partitioning(df: DataFrame, sink: dict[str, Any]) -> DataFrame:
    result = df
    if "coalesce" in sink:
        result = result.coalesce(int(sink["coalesce"]))
    repartition = sink.get("repartition")
    if repartition:
        if isinstance(repartition, int):
            result = result.repartition(repartition)
        elif isinstance(repartition, (list, tuple)):
            result = result.repartition(*repartition)
        elif isinstance(repartition, dict):
            num = repartition.get("num") or repartition.get("partitions")
            cols = repartition.get("cols") or repartition.get("columns") or []
            if num and cols:
                result = result.repartition(int(num), *cols)
            elif num:
                result = result.repartition(int(num))
            elif cols:
                result = result.repartition(*cols)
    return result


def write_batch(df: DataFrame, platform: PlatformBase, sink: dict[str, Any]) -> None:
    sink_type = sink["type"]
    fmt = sink.get("format", "parquet")
    mode = sink.get("mode", "append")
    options = _merge_write_options(sink)
    partition_by = sink.get("partition_by")
    merge_schema = _merge_schema_flag(sink)
    prepared_df = _apply_partitioning(df, sink)
    file_size = sink.get("target_file_size_mb", sink.get("file_size_mb"))
    if file_size is not None:
        options.setdefault("maxRecordsPerFile", int(file_size) * 1024 * 1024)

    if sink_type == "storage":
        uri = platform.normalize_uri(sink["uri"])
        backend = sink.get("backend", platform.name)
        encryption_cfg = sink.get("encryption")
        if encryption_cfg and str(encryption_cfg.get("type", "")).lower() == "pgp":
            if backend == "local":
                _write_with_pgp_local(
                    prepared_df, uri, fmt, mode, options, partition_by, merge_schema, encryption_cfg
                )
                return
            if backend == "aws":
                _write_with_pgp_s3(
                    prepared_df, uri, fmt, mode, options, partition_by, merge_schema, encryption_cfg
                )
                return
            if backend == "gcp":
                _write_with_pgp_gcs(
                    prepared_df, uri, fmt, mode, options, partition_by, merge_schema, encryption_cfg
                )
                return
            if backend == "azure":
                _write_with_pgp_abfs(
                    prepared_df, uri, fmt, mode, options, partition_by, merge_schema, encryption_cfg
                )
                return
            raise RuntimeError(f"encryption.type=pgp no soportado para backend={backend}")
        connector = _storage_connector(backend)
        connector.write(
            prepared_df,
            uri,
            fmt,
            mode,
            options,
            partition_by=partition_by,
            merge_schema=merge_schema,
        )
        return

    if sink_type == "warehouse":
        engine = sink.get("engine")
        if engine in {"synapse", "redshift", "postgres", "mysql", "sqlserver"}:
            jdbc.write(prepared_df, {**sink, "options": options})
            return
        if engine == "bigquery":
            writer = prepared_df.write.format("bigquery").mode(mode)
            for key, value in options.items():
                writer = writer.option(key, value)
            temp_bucket = sink.get("temporary_gcs_bucket") or sink.get("temporaryGcsBucket")
            if temp_bucket:
                writer = writer.option("temporaryGcsBucket", temp_bucket)
            intermediate = sink.get("intermediate_format") or sink.get("intermediateFormat")
            if intermediate:
                writer = writer.option("intermediateFormat", intermediate)
            writer.save(sink["table"])
            return
        raise ValueError(f"Motor de warehouse no soportado: {engine}")

    if sink_type == "nosql":
        _write_nosql(prepared_df, sink)
        return

    raise ValueError(f"Tipo de destino no soportado: {sink_type}")


def _write_with_pgp_local(
    df: DataFrame,
    dest_uri: str,
    fmt: str,
    mode: str,
    options: dict[str, Any],
    partition_by: list[str] | None,
    merge_schema: bool | None,
    encryption_cfg: dict[str, Any],
) -> None:
    import shutil
    import subprocess
    from pathlib import Path

    tmp_dir = Path(dest_uri).with_name(Path(dest_uri).name + ".__plain_tmp")
    tmp_dir_str = str(tmp_dir)
    local.write(
        df,
        tmp_dir_str,
        fmt,
        mode,
        options,
        partition_by=partition_by,
        merge_schema=merge_schema,
    )
    recipient = encryption_cfg.get("recipient")
    output_ext = encryption_cfg.get("output_ext", ".pgp")
    if not recipient:
        raise ValueError("PGP requiere 'recipient' en sink.encryption")
    if shutil.which("gpg") is None:
        raise RuntimeError("gpg no disponible en el sistema para cifrado PGP local")
    dest = Path(dest_uri)
    dest.mkdir(parents=True, exist_ok=True)
    for src in tmp_dir.rglob("*"):
        if src.is_dir():
            continue
        rel = src.relative_to(tmp_dir)
        out_path = dest / rel
        out_path.parent.mkdir(parents=True, exist_ok=True)
        enc_path = Path(str(out_path) + output_ext)
        subprocess.run(
            [
                "gpg",
                "--yes",
                "--batch",
                "--encrypt",
                "--recipient",
                str(recipient),
                "-o",
                str(enc_path),
                str(src),
            ],
            check=True,
        )
    shutil.rmtree(tmp_dir, ignore_errors=True)


def _write_with_pgp_s3(
    df: DataFrame,
    dest_uri: str,
    fmt: str,
    mode: str,
    options: dict[str, Any],
    partition_by: list[str] | None,
    merge_schema: bool | None,
    encryption_cfg: dict[str, Any],
) -> None:
    import os
    import tempfile
    import shutil
    import subprocess
    from urllib.parse import urlparse
    from pathlib import Path

    if boto3 is None:
        raise RuntimeError("boto3 es requerido para cifrado PGP y subida a S3")
    parsed = urlparse(dest_uri.replace("s3a://", "s3://", 1))
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    recipient = encryption_cfg.get("recipient")
    output_ext = encryption_cfg.get("output_ext", ".pgp")
    if not recipient:
        raise ValueError("PGP requiere 'recipient' en sink.encryption")
    if shutil.which("gpg") is None:
        raise RuntimeError("gpg no disponible en el sistema para cifrado PGP")
    s3 = boto3.client("s3")
    if mode == "overwrite" and prefix:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
    tmp_dir = Path(tempfile.mkdtemp(prefix="datacore-pgp-s3-"))
    local.write(
        df,
        str(tmp_dir),
        fmt,
        "overwrite",
        options,
        partition_by=partition_by,
        merge_schema=merge_schema,
    )
    for src in tmp_dir.rglob("*"):
        if src.is_dir():
            continue
        rel = src.relative_to(tmp_dir).as_posix()
        enc_path = src.with_suffix(src.suffix + output_ext)
        subprocess.run(
            [
                "gpg",
                "--yes",
                "--batch",
                "--encrypt",
                "--recipient",
                str(recipient),
                "-o",
                str(enc_path),
                str(src),
            ],
            check=True,
        )
        key = f"{prefix}/{rel}{output_ext}".rstrip("/")
        extra_args: dict[str, Any] = {}
        if "serverSideEncryption" in options:
            extra_args["ServerSideEncryption"] = options["serverSideEncryption"]
        if "sseKmsKeyId" in options:
            extra_args["SSEKMSKeyId"] = options["sseKmsKeyId"]
        s3.upload_file(str(enc_path), bucket, key, ExtraArgs=extra_args or None)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def _write_with_pgp_gcs(
    df: DataFrame,
    dest_uri: str,
    fmt: str,
    mode: str,
    options: dict[str, Any],
    partition_by: list[str] | None,
    merge_schema: bool | None,
    encryption_cfg: dict[str, Any],
) -> None:
    import importlib
    import tempfile
    import shutil
    import subprocess
    from urllib.parse import urlparse
    from pathlib import Path

    if importlib.util.find_spec("google.cloud.storage") is None:
        raise RuntimeError("google-cloud-storage es requerido para PGP en GCS")
    from google.cloud import storage  # type: ignore

    parsed = urlparse(dest_uri)
    bucket_name = parsed.netloc
    prefix = parsed.path.lstrip("/")
    recipient = encryption_cfg.get("recipient")
    output_ext = encryption_cfg.get("output_ext", ".pgp")
    if not recipient:
        raise ValueError("PGP requiere 'recipient' en sink.encryption")
    if shutil.which("gpg") is None:
        raise RuntimeError("gpg no disponible en el sistema para cifrado PGP")
    client = storage.Client()  # credenciales manejadas por entorno
    bucket = client.bucket(bucket_name)
    if mode == "overwrite" and prefix:
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            blob.delete()
    tmp_dir = Path(tempfile.mkdtemp(prefix="datacore-pgp-gcs-"))
    local.write(
        df,
        str(tmp_dir),
        fmt,
        "overwrite",
        options,
        partition_by=partition_by,
        merge_schema=merge_schema,
    )
    for src in tmp_dir.rglob("*"):
        if src.is_dir():
            continue
        rel = src.relative_to(tmp_dir).as_posix()
        enc_path = src.with_suffix(src.suffix + output_ext)
        subprocess.run(
            [
                "gpg",
                "--yes",
                "--batch",
                "--encrypt",
                "--recipient",
                str(recipient),
                "-o",
                str(enc_path),
                str(src),
            ],
            check=True,
        )
        blob = bucket.blob(f"{prefix}/{rel}{output_ext}".rstrip("/"))
        blob.upload_from_filename(str(enc_path))
    shutil.rmtree(tmp_dir, ignore_errors=True)


def _write_with_pgp_abfs(
    df: DataFrame,
    dest_uri: str,
    fmt: str,
    mode: str,
    options: dict[str, Any],
    partition_by: list[str] | None,
    merge_schema: bool | None,
    encryption_cfg: dict[str, Any],
) -> None:
    import importlib
    import tempfile
    import shutil
    import subprocess
    from urllib.parse import urlparse
    from pathlib import Path

    if importlib.util.find_spec("azure.storage.blob") is None:
        raise RuntimeError("azure-storage-blob es requerido para PGP en ABFS/ADLS")
    from azure.storage.blob import BlobServiceClient  # type: ignore

    parsed = urlparse(dest_uri)
    # abfss://<filesystem>@<account>.dfs.core.windows.net/<path>
    filesystem = parsed.netloc.split("@")[0]
    account = parsed.netloc.split("@")[1].split(".")[0] if "@" in parsed.netloc else parsed.netloc
    path_prefix = parsed.path.lstrip("/")
    recipient = encryption_cfg.get("recipient")
    output_ext = encryption_cfg.get("output_ext", ".pgp")
    if not recipient:
        raise ValueError("PGP requiere 'recipient' en sink.encryption")
    if shutil.which("gpg") is None:
        raise RuntimeError("gpg no disponible en el sistema para cifrado PGP")
    conn_str = options.get("connection_string") or options.get("connectionString")
    if not conn_str:
        raise ValueError("Se requiere connectionString para subir a Azure Blob/ADLS con PGP")
    service = BlobServiceClient.from_connection_string(conn_str)
    container = service.get_container_client(filesystem)
    if mode == "overwrite" and path_prefix:
        blobs = container.list_blobs(name_starts_with=path_prefix)
        for blob in blobs:
            container.delete_blob(blob)
    tmp_dir = Path(tempfile.mkdtemp(prefix="datacore-pgp-abfs-"))
    local.write(
        df,
        str(tmp_dir),
        fmt,
        "overwrite",
        options,
        partition_by=partition_by,
        merge_schema=merge_schema,
    )
    for src in tmp_dir.rglob("*"):
        if src.is_dir():
            continue
        rel = src.relative_to(tmp_dir).as_posix()
        enc_path = src.with_suffix(src.suffix + output_ext)
        subprocess.run(
            [
                "gpg",
                "--yes",
                "--batch",
                "--encrypt",
                "--recipient",
                str(recipient),
                "-o",
                str(enc_path),
                str(src),
            ],
            check=True,
        )
        blob_name = f"{path_prefix}/{rel}{output_ext}".rstrip("/")
        with open(enc_path, "rb") as fh:
            container.upload_blob(name=blob_name, data=fh, overwrite=True)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def _write_nosql(df: DataFrame, sink: dict[str, Any]) -> None:
    engine = sink.get("engine")
    options = _merge_write_options(sink)
    if engine == "cosmosdb":
        writer = df.write.format(options.get("format", "cosmos.oltp")).mode(
            sink.get("mode", "append")
        )
        for key, value in options.items():
            writer = writer.option(key, value)
        writer.save()
        return
    if engine == "dynamodb":
        _write_dynamodb(df, sink, options)
        return
    raise ValueError(f"Motor NoSQL no soportado: {engine}")


def _write_dynamodb(df: DataFrame, sink: dict[str, Any], options: dict[str, Any]) -> None:
    if boto3 is None:  # pragma: no cover - requiere dependencia externa
        raise RuntimeError("boto3 es requerido para escribir en DynamoDB")
    table_name = sink["table"]
    region = options.get("region")
    resource = boto3.resource("dynamodb", region_name=region)
    table = resource.Table(table_name)
    batch_size = int(sink.get("batch_size", 25))
    if sink.get("glue_catalog"):
        LOGGER.info(
            "Actualización de Glue Catalog solicitada para %s, no-op en ejecución local",
            sink["glue_catalog"],
        )

    items = (row.asDict(recursive=True) for row in df.toLocalIterator())
    buffer: list[dict[str, Any]] = []
    with table.batch_writer(overwrite_by_pkeys=sink.get("keys")) as batch:
        for item in items:
            buffer.append(item)
            if len(buffer) >= batch_size:
                for chunk in buffer:
                    batch.put_item(Item=chunk)
                buffer.clear()
        for chunk in buffer:
            batch.put_item(Item=chunk)


def _stream_writer_common(
    df: DataFrame,
    options: dict[str, Any],
    fmt: str,
    mode: str,
    checkpoint: str,
    trigger: str | None = None,
) -> None:
    stream_options = {k: v for k, v in options.items() if k != "await_termination"}
    await_termination = bool(options.get("await_termination", False))
    writer = (
        df.writeStream.format(fmt)
        .outputMode(mode)
        .options(**stream_options)
        .option("checkpointLocation", checkpoint)
    )
    if trigger:
        writer = writer.trigger(processingTime=trigger)
    query = writer.start()
    query.awaitTermination(await_termination)


def write_stream(
    df: DataFrame,
    platform: PlatformBase,
    sink: dict[str, Any],
    checkpoint: str,
    trigger: str | None = None,
) -> None:
    sink_type = sink["type"]
    mode = sink.get("mode", "append")
    options = _merge_write_options(sink)

    if sink_type == "storage":
        fmt = sink.get("format", "parquet")
        uri = platform.normalize_uri(sink["uri"])
        options.setdefault("path", uri)
        _stream_writer_common(df, options, fmt, mode, checkpoint, trigger)
        return

    if sink_type == "kafka":
        options.setdefault("topic", sink.get("topic"))
        _stream_writer_common(df, options, "kafka", mode, checkpoint, trigger)
        return

    if sink_type == "event_hubs":
        if "eventHubs.connectionString" not in options:
            options["eventHubs.connectionString"] = sink.get("connectionString")
        _stream_writer_common(df, options, "eventhubs", mode, checkpoint, trigger)
        return

    raise ValueError(f"Streaming no soportado para {sink_type}")


def write_rejects(
    df: DataFrame,
    platform: PlatformBase,
    sink: dict[str, Any],
    *,
    layer: str,
    dataset: str,
    environment: str,
    fmt: str = "parquet",
) -> None:
    if "uri" not in sink:
        LOGGER.warning("No se pueden almacenar rejects para un sink sin URI explícita")
        return
    base_uri = sink["uri"].rstrip("/")
    rejects_uri = f"{base_uri}/_rejects"
    backend = sink.get("backend", platform.name)
    connector = _storage_connector(backend)
    options = sink.get("reject_options", {})
    connector.write(
        df,
        platform.normalize_uri(rejects_uri),
        fmt,
        "append",
        options,
        partition_by=None,
        merge_schema=True,
    )


def write_metrics(
    spark,
    platform: PlatformBase,
    sink: dict[str, Any],
    *,
    layer: str,
    dataset: str,
    environment: str,
    run_id: str,
    metrics: dict[str, Any],
) -> None:
    if "uri" not in sink:
        LOGGER.warning("No se pueden almacenar métricas para un sink sin URI explícita")
        return
    base_uri = sink["uri"].rstrip("/")
    metrics_uri = f"{base_uri}/_metrics/{run_id}.json"
    backend = sink.get("backend", platform.name)
    connector = _storage_connector(backend)
    metrics_json = json.dumps(metrics, ensure_ascii=False)
    metrics_df = spark.createDataFrame([(metrics_json,)], ["value"]).coalesce(
        1
    )  # un solo archivo por ejecución
    connector.write(
        metrics_df,
        platform.normalize_uri(metrics_uri),
        "text",
        "overwrite",
        {"compression": sink.get("metrics_compression", "none")},
        partition_by=None,
        merge_schema=True,
    )
