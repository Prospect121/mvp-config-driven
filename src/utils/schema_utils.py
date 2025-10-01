"""
Utilidades para gestión de esquemas de datos.
"""
import json
import re
import logging
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, DateType, TimestampType, DecimalType

logger = logging.getLogger(__name__)

class SchemaManager:
    """Gestor de esquemas de datos"""
    
    # Mapeo de tipos
    TYPE_ALIASES = {
        "string": "string", "str": "string", "varchar": "string", "char": "string",
        "int": "int", "integer": "int", "bigint": "bigint", "long": "bigint",
        "double": "double", "float": "double", "boolean": "boolean", "bool": "boolean",
        "date": "date", "datetime": "timestamp", "timestamp": "timestamp", "timestamptz": "timestamp",
        "number": "double", "numeric": "double", "decimal": "decimal(38,18)"
    }
    
    def __init__(self):
        self.decimal_full_pattern = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)
        self.decimal_prec_pattern = re.compile(r"^decimal\(\s*(\d+)\s*\)$", re.IGNORECASE)
    
    def normalize_type(self, type_str: str) -> str:
        """
        Normaliza un tipo de dato a formato estándar.
        
        Args:
            type_str: Tipo de dato como string
            
        Returns:
            Tipo normalizado
        """
        raw = (type_str or "").strip().lower()
        
        # Verificar decimal con precisión y escala
        match = self.decimal_full_pattern.match(raw)
        if match:
            precision, scale = int(match.group(1)), int(match.group(2))
            if scale > precision:
                raise ValueError(f"decimal({precision},{scale}) inválido: scale > precision")
            return f"decimal({precision},{scale})"
        
        # Verificar decimal solo con precisión
        match = self.decimal_prec_pattern.match(raw)
        if match:
            return f"decimal({int(match.group(1))},0)"
        
        # Verificar aliases
        if raw in self.TYPE_ALIASES:
            return self.TYPE_ALIASES[raw]
        
        # Verificar si ya es un decimal válido
        if raw.startswith("decimal(") and raw.endswith(")"):
            return raw
        
        raise ValueError(f"Tipo no reconocido: {type_str}")
    
    def load_json_schema(self, schema_path: str) -> Dict[str, Any]:
        """
        Carga esquema desde archivo JSON.
        
        Args:
            schema_path: Ruta al archivo de esquema
            
        Returns:
            Esquema como diccionario
        """
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading schema from {schema_path}: {e}")
            raise
    
    def spark_type_from_json(self, type_def: Dict[str, Any]) -> Any:
        """
        Convierte definición de tipo JSON a tipo Spark.
        
        Args:
            type_def: Definición de tipo desde JSON schema
            
        Returns:
            Tipo de Spark correspondiente
        """
        type_name = type_def.get('type', 'string').lower()
        
        type_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'bigint': LongType(),
            'double': DoubleType(),
            'boolean': BooleanType(),
            'date': DateType(),
            'timestamp': TimestampType()
        }
        
        if type_name in type_mapping:
            return type_mapping[type_name]
        elif type_name.startswith('decimal'):
            # Extraer precisión y escala del decimal
            match = self.decimal_full_pattern.match(type_name)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                return DecimalType(precision, scale)
            else:
                return DecimalType(38, 18)  # Default
        else:
            logger.warning(f"Unknown type {type_name}, defaulting to StringType")
            return StringType()
    
    def create_spark_schema(self, json_schema: Dict[str, Any]) -> StructType:
        """
        Crea esquema de Spark desde definición JSON.
        
        Args:
            json_schema: Esquema en formato JSON
            
        Returns:
            StructType de Spark
        """
        fields = []
        
        for field_def in json_schema.get('fields', []):
            field_name = field_def['name']
            field_type = self.spark_type_from_json(field_def)
            nullable = field_def.get('nullable', True)
            
            fields.append(StructField(field_name, field_type, nullable))
        
        return StructType(fields)
    
    def enforce_schema(self, df: DataFrame, schema_path: str, mode: str = "strict") -> DataFrame:
        """
        Aplica esquema a un DataFrame.
        
        Args:
            df: DataFrame de entrada
            schema_path: Ruta al archivo de esquema
            mode: Modo de aplicación ('strict', 'permissive')
            
        Returns:
            DataFrame con esquema aplicado
        """
        try:
            json_schema = self.load_json_schema(schema_path)
            target_schema = self.create_spark_schema(json_schema)
            
            if mode.lower() == "strict":
                # Modo estricto: aplicar esquema exacto
                df_with_schema = df.select(*[
                    df[field.name].cast(field.dataType).alias(field.name)
                    for field in target_schema.fields
                    if field.name in df.columns
                ])
                
                # Verificar que todas las columnas requeridas estén presentes
                missing_cols = set(field.name for field in target_schema.fields) - set(df.columns)
                if missing_cols:
                    logger.warning(f"Missing columns in strict mode: {missing_cols}")
                
                return df_with_schema
            
            else:
                # Modo permisivo: aplicar conversiones donde sea posible
                for field in target_schema.fields:
                    if field.name in df.columns:
                        df = df.withColumn(field.name, df[field.name].cast(field.dataType))
                
                return df
                
        except Exception as e:
            logger.error(f"Error enforcing schema: {e}")
            if mode.lower() == "strict":
                raise
            else:
                logger.warning("Continuing without schema enforcement")
                return df
    
    def validate_schema_compatibility(self, df: DataFrame, schema_path: str) -> List[str]:
        """
        Valida compatibilidad de esquema sin aplicar cambios.
        
        Args:
            df: DataFrame a validar
            schema_path: Ruta al esquema de referencia
            
        Returns:
            Lista de problemas encontrados
        """
        issues = []
        
        try:
            json_schema = self.load_json_schema(schema_path)
            target_schema = self.create_spark_schema(json_schema)
            
            # Verificar columnas faltantes
            df_cols = set(df.columns)
            schema_cols = set(field.name for field in target_schema.fields)
            
            missing_cols = schema_cols - df_cols
            extra_cols = df_cols - schema_cols
            
            if missing_cols:
                issues.append(f"Missing columns: {missing_cols}")
            
            if extra_cols:
                issues.append(f"Extra columns: {extra_cols}")
            
            # Verificar tipos de datos
            for field in target_schema.fields:
                if field.name in df.columns:
                    df_type = dict(df.dtypes)[field.name]
                    expected_type = field.dataType.simpleString()
                    
                    if df_type != expected_type:
                        issues.append(f"Type mismatch for {field.name}: got {df_type}, expected {expected_type}")
            
        except Exception as e:
            issues.append(f"Schema validation error: {e}")
        
        return issues