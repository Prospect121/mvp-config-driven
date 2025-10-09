"""
Schema Mapper: Convierte schemas JSON a declaraciones DDL SQL
Soporta PostgreSQL con diseño extensible para otras bases de datos
"""

import json
import logging
import yaml
import os
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class DatabaseEngine(Enum):
    """Motores de base de datos soportados"""
    POSTGRESQL = "postgresql"


@dataclass
class ColumnDefinition:
    """Definición de columna para DDL"""
    name: str
    sql_type: str
    nullable: bool = True
    primary_key: bool = False
    constraints: List[str] = None

    def __post_init__(self):
        if self.constraints is None:
            self.constraints = []


class SchemaMapper:
    """Mapea JSON Schema a DDL SQL para diferentes motores de base de datos"""
    
    def __init__(self, engine: DatabaseEngine = DatabaseEngine.POSTGRESQL, config_path: str = None):
        if engine != DatabaseEngine.POSTGRESQL:
            raise ValueError(f"Solo PostgreSQL es soportado, se recibió: {engine}")
        self.engine = engine
        
        # Load configuration
        if config_path is None:
            # Default config path relative to this file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(current_dir, "..", "..", "config", "schema_mapping.yml")
        
        self.config = self._load_config(config_path)
        self.type_mapping = self.config["type_mappings"][engine.value]
        self.limits = self.config["limits"]
        self.pk_config = self.config["primary_key_detection"]
        self.format_mappings = self.config["format_mappings"]
        
        # Load validation and metadata configurations
        self.validation_config = self.config.get("validation", {})
        self.metadata_tables = self.config.get("metadata_tables", {})
        self.default_schemas = self.config.get("default_schemas", {})

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Cargar configuración desde archivo YAML"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading schema mapping config from {config_path}: {e}")
            # Fallback to default configuration
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """Configuración por defecto si no se puede cargar el archivo"""
        return {
            "type_mappings": {
                "postgresql": {
                    "string": "VARCHAR(255)",
                    "string_text": "TEXT",
                    "string_datetime": "TIMESTAMP",
                    "number": "DECIMAL(15,2)",
                    "integer": "INTEGER",
                    "boolean": "BOOLEAN",
                    "array": "JSONB",
                    "object": "JSONB"
                }
            },
            "limits": {
                "varchar_to_text_threshold": 1000,
                "default_varchar_length": 255,
                "decimal_precision": 15,
                "decimal_scale": 2
            },
            "primary_key_detection": {
                "id_suffixes": ["_id", "Id", "ID"],
                "id_names": ["id", "ID", "Id"],
                "require_in_required_fields": True,
                "require_multiple_required_fields": True
            },
            "format_mappings": {
                "date-time": "TIMESTAMP",
                "date": "DATE",
                "time": "TIME",
                "email": "VARCHAR(255)",
                "uri": "VARCHAR(500)",
                "uuid": "UUID"
            },
            "validation": {
                "valid_json_types": ["string", "number", "integer", "boolean", "array", "object", "null"],
                "supported_engines": ["postgresql"]
            },
            "metadata_tables": {
                "schema_versions": "schema_versions",
                "pipeline_executions": "metadata.pipeline_executions",
                "dataset_versions": "metadata.dataset_versions"
            },
            "default_schemas": {
                "postgresql": {
                    "metadata_schema": "metadata",
                    "data_schema": "gold"
                }
            }
        }

    def load_schema(self, schema_path: str) -> Dict[str, Any]:
        """Cargar schema JSON desde archivo"""
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading schema from {schema_path}: {e}")
            raise

    def map_json_type_to_sql(self, property_name: str, property_def: Dict[str, Any]) -> str:
        """Mapear propiedad de JSON schema a tipo SQL"""
        json_type = property_def.get("type")
        
        # Handle union types (e.g., ["string", "null"])
        if isinstance(json_type, list):
            # Take the first non-null type
            json_type = next((t for t in json_type if t != "null"), "string")
        
        # Handle special formats
        if json_type == "string":
            format_type = property_def.get("format")
            if format_type and format_type in self.format_mappings:
                return self.format_mappings[format_type]
            elif property_def.get("pattern"):
                # For patterns, use regular VARCHAR
                return self.type_mapping["string"]
            elif len(property_def.get("enum", [])) > 0:
                # For enums, use VARCHAR
                return self.type_mapping["string"]
            else:
                # Check if it might be a long text field
                max_length = property_def.get("maxLength", self.limits["default_varchar_length"])
                if max_length > self.limits["varchar_to_text_threshold"]:
                    return self.type_mapping["string_text"]
                return self.type_mapping["string"]
        
        return self.type_mapping.get(json_type, self.type_mapping["string"])

    def is_nullable(self, property_name: str, property_def: Dict[str, Any], required_fields: List[str]) -> bool:
        """Determinar si una columna debe ser nullable"""
        # Check if it's in required fields
        if property_name in required_fields:
            return False
        
        # Check if type explicitly includes null
        json_type = property_def.get("type")
        if isinstance(json_type, list) and "null" in json_type:
            return True
        
        return True

    def detect_constraints(self, property_name: str, property_def: Dict[str, Any]) -> List[str]:
        """Detectar constraints adicionales para la columna"""
        constraints = []
        
        # Pattern constraint
        if pattern := property_def.get("pattern"):
            # Note: PostgreSQL uses CHECK constraints for patterns
            constraints.append(f"CHECK ({property_name} ~ '{pattern}')")
        
        # Enum constraint
        if enum_values := property_def.get("enum"):
            enum_str = "', '".join(str(v) for v in enum_values)
            constraints.append(f"CHECK ({property_name} IN ('{enum_str}'))")
        
        # Numeric constraints
        if property_def.get("type") in ["number", "integer"]:
            if minimum := property_def.get("minimum"):
                constraints.append(f"CHECK ({property_name} >= {minimum})")
            if maximum := property_def.get("maximum"):
                constraints.append(f"CHECK ({property_name} <= {maximum})")
        
        return constraints

    def schema_to_columns(self, schema: Dict[str, Any]) -> List[ColumnDefinition]:
        """Convertir JSON schema a lista de definiciones de columnas"""
        properties = schema.get("properties", {})
        required_fields = schema.get("required", [])
        columns = []
        
        for prop_name, prop_def in properties.items():
            sql_type = self.map_json_type_to_sql(prop_name, prop_def)
            nullable = self.is_nullable(prop_name, prop_def, required_fields)
            constraints = self.detect_constraints(prop_name, prop_def)
            
            # Detect primary key based on configuration
            is_pk = self._is_primary_key(prop_name, required_fields)
            
            column = ColumnDefinition(
                name=prop_name,
                sql_type=sql_type,
                nullable=nullable,
                primary_key=is_pk,
                constraints=constraints
            )
            columns.append(column)
        
        return columns

    def _is_primary_key(self, prop_name: str, required_fields: List[str]) -> bool:
        """Detectar si una propiedad es clave primaria basado en configuración"""
        # Check if name matches configured patterns
        if prop_name in self.pk_config["id_names"]:
            name_matches = True
        else:
            name_matches = any(prop_name.endswith(suffix) for suffix in self.pk_config["id_suffixes"])
        
        if not name_matches:
            return False
        
        # Check additional requirements from configuration
        if self.pk_config["require_in_required_fields"] and prop_name not in required_fields:
            return False
        
        if self.pk_config["require_multiple_required_fields"] and len(required_fields) <= 1:
            return False
        
        return True

    def generate_create_table_ddl(self, table_name: str, schema_or_columns, 
                                 schema_version: Optional[str] = None) -> str:
        """Generar DDL CREATE TABLE statement
        
        Args:
            table_name: Nombre de la tabla a crear
            schema_or_columns: Ya sea un dict de JSON schema o List[ColumnDefinition]
            schema_version: Versión opcional del schema para seguimiento
        """
        # Manejar tanto dict de schema como lista de columnas
        if isinstance(schema_or_columns, dict):
            # Es un JSON schema
            columns = self.schema_to_columns(schema_or_columns)
        else:
            # Ya es una lista de ColumnDefinition
            columns = schema_or_columns
        
        column_definitions = []
        primary_keys = []
        
        for column in columns:
            col_def = f"    {column.name} {column.sql_type}"
            
            if not column.nullable:
                col_def += " NOT NULL"
            

            
            if column.primary_key:
                primary_keys.append(column.name)
            
            column_definitions.append(col_def)
        
        # Construir todos los elementos de tabla (columnas, constraints, etc.)
        table_elements = []
        table_elements.extend(column_definitions)
        
        # Agregar constraint de primary key
        if primary_keys:
            pk_constraint = f"    PRIMARY KEY ({', '.join(primary_keys)})"
            table_elements.append(pk_constraint)
        
        # Agregar constraints individuales de columna
        for column in columns:
            for constraint in column.constraints:
                table_elements.append(f"    {constraint}")
        
        # Construir DDL final
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        ddl += ",\n".join(table_elements)
        ddl += "\n);"
        
        # Agregar versión de schema como comentario (PostgreSQL soporta comentarios)
        if schema_version and self.engine == DatabaseEngine.POSTGRESQL:
            ddl += f"\n-- Schema Version: {schema_version}"
        
        return ddl

    def generate_alter_table_ddl(self, table_name: str, old_columns: List[ColumnDefinition], 
                                 new_columns: List[ColumnDefinition]) -> List[str]:
        """Generar declaraciones DDL ALTER TABLE para cambios de schema"""
        ddl_statements = []
        
        # Crear mapas para comparación más fácil
        old_cols_map = {col.name: col for col in old_columns}
        new_cols_map = {col.name: col for col in new_columns}
        
        # Encontrar nuevas columnas (adiciones)
        for col_name, col_def in new_cols_map.items():
            if col_name not in old_cols_map:
                ddl = f"ALTER TABLE {table_name} ADD COLUMN {col_def.name} {col_def.sql_type}"
                if not col_def.nullable:
                    ddl += " NOT NULL"
                ddl += ";"
                ddl_statements.append(ddl)
        
        # Encontrar columnas modificadas (cambios de tipo)
        for col_name, new_col in new_cols_map.items():
            if col_name in old_cols_map:
                old_col = old_cols_map[col_name]
                if old_col.sql_type != new_col.sql_type or old_col.nullable != new_col.nullable:
                    ddl = f"ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE {new_col.sql_type}"
                    if not new_col.nullable and old_col.nullable:
                        ddl += f", ALTER COLUMN {col_name} SET NOT NULL"
                    elif new_col.nullable and not old_col.nullable:
                        ddl += f", ALTER COLUMN {col_name} DROP NOT NULL"
                    ddl += ";"
                    ddl_statements.append(ddl)
        
        return ddl_statements

    def schema_from_file_to_ddl(self, schema_path: str, table_name: str, 
                               schema_version: Optional[str] = None) -> str:
        """Convert JSON schema file to DDL statement"""
        schema = self.load_schema(schema_path)
        columns = self.schema_to_columns(schema)
        return self.generate_create_table_ddl(table_name, columns, schema_version)
    
    def schema_to_ddl(self, schema_dict: Dict, table_name: str, 
                     schema_version: Optional[str] = None) -> str:
        """Convert JSON schema dictionary to DDL statement"""
        columns = self.schema_to_columns(schema_dict)
        return self.generate_create_table_ddl(table_name, columns, schema_version)
    
    def validate_json_type(self, json_type: str) -> bool:
        """Validar si un tipo JSON es válido según la configuración"""
        valid_types = self.validation_config.get("valid_json_types", [])
        return json_type in valid_types
    
    def validate_engine_support(self, engine: str) -> bool:
        """Validar si un motor de base de datos es soportado"""
        supported_engines = self.validation_config.get("supported_engines", [])
        return engine in supported_engines
    
    def get_metadata_table_name(self, table_type: str) -> str:
        """Obtener el nombre de tabla de metadata según la configuración"""
        return self.metadata_tables.get(table_type, table_type)
    
    def get_default_schema(self, schema_type: str) -> str:
        """Obtener el esquema por defecto según la configuración"""
        engine_schemas = self.default_schemas.get(self.engine.value, {})
        return engine_schemas.get(schema_type, schema_type)
    
    def validate_schema(self, schema: Dict[str, Any]) -> List[str]:
        """Validar un schema JSON y retornar lista de errores"""
        errors = []
        
        # Validar que el schema tenga la estructura básica
        if not isinstance(schema, dict):
            errors.append("Schema debe ser un diccionario")
            return errors
        
        # Validar propiedades
        properties = schema.get("properties", {})
        if not isinstance(properties, dict):
            errors.append("'properties' debe ser un diccionario")
            return errors
        
        # Validar tipos de cada propiedad
        for prop_name, prop_def in properties.items():
            if not isinstance(prop_def, dict):
                errors.append(f"Propiedad '{prop_name}' debe ser un diccionario")
                continue
            
            json_type = prop_def.get("type")
            if json_type:
                if isinstance(json_type, list):
                    # Validar cada tipo en la lista
                    for t in json_type:
                        if not self.validate_json_type(t):
                            errors.append(f"Tipo JSON inválido '{t}' en propiedad '{prop_name}'")
                else:
                    # Validar tipo único
                    if not self.validate_json_type(json_type):
                        errors.append(f"Tipo JSON inválido '{json_type}' en propiedad '{prop_name}'")
        
        return errors


def json_schema_to_ddl(schema_path: str, table_name: str, engine: DatabaseEngine = DatabaseEngine.POSTGRESQL) -> str:
    """Convertir archivo JSON Schema a declaración DDL"""
    mapper = SchemaMapper(engine)
    return mapper.schema_from_file_to_ddl(schema_path, table_name)


def json_schema_dict_to_ddl(schema_dict: Dict, table_name: str, engine: DatabaseEngine = DatabaseEngine.POSTGRESQL) -> str:
    """Convertir diccionario JSON Schema a declaración DDL"""
    mapper = SchemaMapper(engine)
    return mapper.generate_create_table_ddl(table_name, schema_dict)


def main():
    """Ejemplo de uso"""
    # Ruta de schema de ejemplo
    schema_path = "config/datasets/finanzas/payments_v1/schema.json"
    table_name = "payments_v1"
    
    try:
        # Generar DDL desde archivo de schema
        ddl = json_schema_to_ddl(schema_path, table_name)
        print("DDL generado:")
        print(ddl)
        
        # Ejemplo: Generar DDL desde diccionario de schema
        example_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string", "format": "uuid"},
                "amount": {"type": "number"},
                "currency": {"type": "string", "enum": ["USD", "EUR", "GBP"]},
                "created_at": {"type": "string", "format": "date-time"}
            },
            "required": ["id", "amount", "currency"]
        }
        
        ddl_from_dict = json_schema_dict_to_ddl(example_schema, "example_payments")
        print("\nDDL desde diccionario:")
        print(ddl_from_dict)
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()