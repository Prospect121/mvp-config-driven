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
    """Parsear string o lista de ordenamiento a lista de expresiones de columnas Spark."""
    from pyspark.sql.functions import col
    
    if not order_input:
        return []
    
    def parse_single_order(order_str):
        """Parse a single order expression like 'column_name desc' or 'column_name'"""
        order_str = str(order_str).strip()
        parts = order_str.split()
        
        if len(parts) == 1:
            # Solo nombre de columna, orden ascendente por defecto
            return col(parts[0])
        elif len(parts) == 2:
            # Nombre de columna + dirección
            col_name, direction = parts[0], parts[1].lower()
            if direction in ['desc', 'descending']:
                return col(col_name).desc()
            else:
                return col(col_name).asc()
        else:
            # Si hay más partes, tomar la primera como columna y la última como dirección
            col_name = parts[0]
            direction = parts[-1].lower()
            if direction in ['desc', 'descending']:
                return col(col_name).desc()
            else:
                return col(col_name).asc()
    
    # Si ya es una lista, procesarla
    if isinstance(order_input, list):
        return [parse_single_order(item) for item in order_input]
    
    # Si es un string, dividirlo por comas y procesar
    if isinstance(order_input, str):
        return [parse_single_order(item) for item in order_input.split(",")]
    
    # Para cualquier otro tipo, convertir a string y procesar
    return [parse_single_order(order_input)]

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