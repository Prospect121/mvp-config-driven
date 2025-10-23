"""
Módulo común con configuraciones y funciones compartidas entre pipelines.
"""

import os
import re
from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import *

# Expresiones regulares para tipos de datos
DECIMAL_FULL = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)
DECIMAL_PREC = re.compile(r"^decimal\(\s*(\d+)\s*\)$", re.IGNORECASE)

# Aliases de tipos de datos
ALIASES = {
    "string": "StringType()",
    "int": "IntegerType()",
    "integer": "IntegerType()",
    "long": "LongType()",
    "double": "DoubleType()",
    "float": "FloatType()",
    "boolean": "BooleanType()",
    "timestamp": "TimestampType()",
    "date": "DateType()"
}

def norm_type(raw: str) -> str:
    """Normalizar tipo de dato desde string a tipo Spark SQL."""
    raw = raw.strip()
    m = DECIMAL_FULL.match(raw)
    if m:
        precision, scale = m.groups()
        return f"DecimalType({precision}, {scale})"
    
    m = DECIMAL_PREC.match(raw)
    if m:
        return f"DecimalType({m.group(1)}, 0)"
    
    if raw in ALIASES: 
        return ALIASES[raw]
    
    return f"StringType()  # Tipo desconocido: {raw}"

def parse_order(order_input):
    """Parsear especificaciones de ordenamiento a lista de expresiones de columnas Spark.

    Soporta:
    - string: "col1 desc, col2 asc"
    - lista de strings: ["col1 desc", "col2"]
    - lista de dicts: [{column: col1, dir: desc}, {column: col2}]
    - dict único: {column: col1, dir: desc}
    """
    from pyspark.sql.functions import col

    if not order_input:
        return []

    def parse_single_order(order_spec):
        """Parsea una sola especificación de orden."""
        # Dict: {column, dir}
        if isinstance(order_spec, dict):
            col_name = (
                order_spec.get('column')
                or order_spec.get('col')
                or order_spec.get('name')
            )
            direction = str(order_spec.get('dir') or order_spec.get('direction') or 'asc').lower()
            if not col_name:
                # Fallback: ignorar entrada inválida
                return None
            return col(col_name).desc() if direction in ['desc', 'descending'] else col(col_name).asc()

        # String: "col desc" o "col"
        order_str = str(order_spec).strip()
        parts = order_str.split()

        if len(parts) == 1:
            return col(parts[0])
        elif len(parts) >= 2:
            col_name, direction = parts[0], parts[-1].lower()
            return col(col_name).desc() if direction in ['desc', 'descending'] else col(col_name).asc()

    # Lista: procesar cada elemento
    if isinstance(order_input, list):
        parsed = [parse_single_order(item) for item in order_input]
        return [p for p in parsed if p is not None]

    # String: dividir por comas y procesar
    if isinstance(order_input, str):
        return [parse_single_order(item) for item in order_input.split(',')]

    # Dict único u otro tipo: intentar parsear directamente
    single = parse_single_order(order_input)
    return [single] if single is not None else []

def safe_cast(df: DataFrame, column: str, target_type: str, format_hint: Optional[str] = None, on_error: Optional[str] = None) -> DataFrame:
    """Aplicar cast seguro con manejo de errores y soporte para format_hint."""
    from pyspark.sql.functions import to_timestamp, to_date
    
    def apply_cast():
        # Si es timestamp o date y hay format_hint, usar funciones específicas
        if target_type.lower() in ['timestamp', 'timestamptype'] and format_hint:
            return df.withColumn(column, to_timestamp(col(column), format_hint))
        elif target_type.lower() in ['date', 'datetype'] and format_hint:
            return df.withColumn(column, to_date(col(column), format_hint))
        else:
            # Cast normal
            return df.withColumn(column, col(column).cast(target_type))
    
    if on_error == "null":
        # Si hay error, devolver null
        try:
            return apply_cast()
        except:
            return df.withColumn(column, lit(None).cast(target_type))
    elif on_error == "skip":
        # Si hay error, mantener valor original
        try:
            return apply_cast()
        except:
            return df
    else:
        # Cast normal (puede fallar)
        return apply_cast()

def maybe_config_s3a(spark, path: str, env: Dict[str, Any]) -> str:
    """Configurar S3A si la ruta lo requiere."""
    if not path.startswith("s3a://"):
        return path
    
    # Configurar S3A usando variables de entorno o valores por defecto
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minio")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio12345")
    endpoint = env.get("s3a_endpoint", os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000"))
    
    print(f"[S3A] Configuring S3A for path: {path}")
    print(f"[S3A] Access Key: {access_key}")
    print(f"[S3A] Endpoint: {endpoint}")
    
    spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", endpoint)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    return path

# Nuevo helper: configurar ABFS OAuth por host y limpiar claves accidentales

def maybe_config_abfs(spark, path: str, env: Dict[str, Any]) -> str:
    """Asegura OAuth para rutas ABFS y limpia cualquier fs.azure.account.key."""
    if not (path and (path.startswith("abfs://") or path.startswith("abfss://"))):
        return path
    try:
        scheme = "abfs://" if path.startswith("abfs://") else "abfss://"
        remainder = path[len(scheme):]
        authority = remainder.split('/', 1)[0]
        host = authority.split('@')[1] if '@' in authority else authority
        tenant = os.environ.get("AZURE_TENANT_ID")
        client_id = os.environ.get("AZURE_CLIENT_ID")
        client_secret = os.environ.get("AZURE_CLIENT_SECRET")
        if tenant and client_id and client_secret:
            # fs.azure.*
            spark.conf.set(f"fs.azure.account.auth.type.{host}", "OAuth")
            spark.conf.set(f"fs.azure.account.oauth.provider.type.{host}", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            spark.conf.set(f"fs.azure.account.oauth2.client.id.{host}", client_id)
            spark.conf.set(f"fs.azure.account.oauth2.client.secret.{host}", client_secret)
            spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{host}", f"https://login.microsoftonline.com/{tenant}/oauth2/token")
            # spark.hadoop.fs.azure.* (refuerzo)
            spark.conf.set(f"spark.hadoop.fs.azure.account.auth.type.{host}", "OAuth")
            spark.conf.set(f"spark.hadoop.fs.azure.account.oauth.provider.type.{host}", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            spark.conf.set(f"spark.hadoop.fs.azure.account.oauth2.client.id.{host}", client_id)
            spark.conf.set(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{host}", client_secret)
            spark.conf.set(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{host}", f"https://login.microsoftonline.com/{tenant}/oauth2/token")
        # Limpiar claves accidentales
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        for k in [
            "fs.azure.account.key",
            f"fs.azure.account.key.{host}",
            f"fs.azure.account.key.{host}.dfs.core.windows.net",
            f"spark.hadoop.fs.azure.account.key.{host}",
            f"spark.hadoop.fs.azure.account.key.{host}.dfs.core.windows.net",
        ]:
            try:
                hconf.unset(k)
            except Exception:
                pass
        print(f"[azure] OAuth aplicado para {host}; claves fs.azure.account.key limpiadas")
    except Exception as e:
        print(f"[azure] ABFS config fallo: {e}")
    return path