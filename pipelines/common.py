"""
Módulo común con configuraciones y funciones compartidas entre pipelines.
"""

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

