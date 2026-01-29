#!/usr/bin/env python3
"""
============================================================================
Spark Pipeline Job - Datacore ETL Engine
============================================================================

Script ejecutado via spark-submit para correr pipelines Datacore.

Soporta configuraciones desde:
  - Rutas locales: /opt/spark/work-dir/config.yml
  - Rutas S3A: s3a://configs/pipelines/config.yml

VARIABLES DE ENTORNO REQUERIDAS:
  - MINIO_ENDPOINT: URL del endpoint MinIO (ej: http://lakehouse-minio:9000)
  - MINIO_ACCESS_KEY: Access key de MinIO
  - MINIO_SECRET_KEY: Secret key de MinIO

EJEMPLO DE USO:
  spark-submit --py-files datacore.whl spark_pipeline_job.py \\
    --layer raw --config s3a://configs/pipelines/ventas.yml

============================================================================
"""
import sys
import argparse
import yaml
import logging
import os


def get_required_env(name: str, description: str) -> str:
    """
    Obtiene una variable de entorno requerida.
    Lanza error descriptivo si no está configurada.
    """
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Variable de entorno requerida no configurada: {name}\n"
            f"Descripción: {description}\n"
            f"Configúrala en el DAG de Airflow o en docker-compose.yml"
        )
    return value


def load_config_from_s3(s3_path: str) -> dict:
    """
    Carga configuración YAML desde S3/MinIO usando boto3.

    Las credenciales se obtienen EXCLUSIVAMENTE de variables de entorno.
    No hay valores por defecto hardcodeados por seguridad.
    """
    import boto3
    from botocore.client import Config

    # Parsear s3a://bucket/key
    path = s3_path.replace("s3a://", "").replace("s3://", "")
    bucket = path.split("/")[0]
    key = "/".join(path.split("/")[1:])

    # Obtener credenciales de variables de entorno (REQUERIDAS)
    endpoint = get_required_env("MINIO_ENDPOINT", "URL del endpoint MinIO")
    access_key = get_required_env("MINIO_ACCESS_KEY", "Access key de MinIO")
    secret_key = get_required_env("MINIO_SECRET_KEY", "Secret key de MinIO")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )

    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    return yaml.safe_load(content)


def load_config(config_path: str) -> dict:
    """Carga configuración desde archivo local o S3."""
    if config_path.startswith("s3a://") or config_path.startswith("s3://"):
        return load_config_from_s3(config_path)
    else:
        with open(config_path, "r", encoding="utf-8") as fp:
            return yaml.safe_load(fp)


def main():
    parser = argparse.ArgumentParser(description="Ejecutar pipeline datacore")
    parser.add_argument("--layer", required=True, choices=["raw", "bronze", "silver", "gold"])
    parser.add_argument(
        "--config", required=True, help="Path al archivo de configuracion (local o s3a://)"
    )
    parser.add_argument("--platform", default="local", help="Plataforma")
    parser.add_argument("--env", default="dev", help="Entorno")
    args = parser.parse_args()

    # Configurar logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    logger.info(f"Ejecutando capa: {args.layer}")
    logger.info(f"Config: {args.config}")

    # Cargar configuracion (soporta local y S3)
    logger.info("Cargando configuracion...")
    config = load_config(args.config)
    logger.info(f"Proyecto: {config.get('project', 'unknown')}")

    # Importar datacore (instalado via --py-files)
    from datacore.config.validation import validate_config
    from datacore.core.engine import run_layer_plan

    # Validar configuracion
    logger.info("Validando configuracion...")
    validate_config(config)

    # Ejecutar el pipeline
    logger.info(f"Ejecutando run_layer_plan para capa: {args.layer}")
    results = run_layer_plan(
        layer=args.layer,
        config=config,
        platform_name=args.platform,
        environment=args.env,
        dry_run=False,
        fail_fast=False,
    )

    # Revisar resultados
    datasets = results.get("datasets", [])
    failed = sum(1 for d in datasets if d.get("status") == "failed")
    succeeded = sum(1 for d in datasets if d.get("status") == "success")

    logger.info(f"Resultados: {succeeded} exitosos, {failed} fallidos")

    if failed > 0:
        logger.error(f"Pipeline fallido: {results}")
        sys.exit(1)

    logger.info(f"Pipeline completado exitosamente")
    sys.exit(0)


if __name__ == "__main__":
    main()
