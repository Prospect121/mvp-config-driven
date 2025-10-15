"""
Gestor de Base de Datos: Maneja conexiones de base de datos y operaciones de tablas
Soporta creación dinámica de tablas y versionado de schemas
"""

import logging
import hashlib
import json
import yaml
import os
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, DateTime, Integer
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from pyspark.sql import DataFrame

from .schema_mapper import SchemaMapper, DatabaseEngine, ColumnDefinition

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Configuración de conexión a base de datos"""
    engine_type: DatabaseEngine
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: Optional[str] = None
    connection_params: Optional[Dict[str, Any]] = None

    def get_connection_string(self) -> str:
        """Generar string de conexión SQLAlchemy"""
        if self.engine_type != DatabaseEngine.POSTGRESQL:
            raise ValueError(f"Solo PostgreSQL es soportado, se recibió: {self.engine_type}")
            
        base_url = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        
        # Add connection parameters if provided
        if self.connection_params:
            params = "&".join([f"{k}={v}" for k, v in self.connection_params.items()])
            base_url += f"?{params}"
        
        return base_url

    def get_jdbc_connection_string(self) -> str:
        """Generate PostgreSQL JDBC connection string for Spark"""
        if self.engine_type != DatabaseEngine.POSTGRESQL:
            raise ValueError(f"Solo PostgreSQL es soportado, se recibió: {self.engine_type}")
            
        base_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        
        # Add connection parameters if provided
        if self.connection_params:
            params = "&".join([f"{k}={v}" for k, v in self.connection_params.items()])
            base_url += f"?{params}"
        
        return base_url


@dataclass
class SchemaVersion:
    """Seguimiento de versión de schema"""
    table_name: str
    schema_hash: str
    schema_version: str
    created_at: datetime
    schema_content: str


class DatabaseManager:
    """Gestiona conexiones de base de datos y operaciones de tablas"""
    
    def __init__(self, config: DatabaseConfig):
        """Inicializar gestor de base de datos con configuración"""
        self.config = config
        self.engine = None
        self.schema_mapper = SchemaMapper(config.engine_type)
        self._connect()
        self._ensure_schema_tracking_table()
    
    def get_connection(self):
        """Get database connection (context manager)"""
        return self.engine.connect()

    def _connect(self):
        """Establecer conexión a base de datos"""
        try:
            connection_string = self.config.get_connection_string()
            self.engine = create_engine(connection_string, echo=False)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Conectado a base de datos {self.config.engine_type.value}")
        except Exception as e:
            logger.error(f"Error al conectar a base de datos: {e}")
            raise

    def _ensure_schema_tracking_table(self):
        """Crear tabla de seguimiento de versiones de schema si no existe"""
        # Crear esquemas por defecto si no existen
        self._ensure_default_schemas()
        
        # Obtener nombre de tabla desde configuración
        table_name = self.schema_mapper.get_metadata_table_name("schema_versions")
        
        # PostgreSQL DDL for schema tracking
        tracking_table_ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL,
            schema_hash VARCHAR(64) NOT NULL,
            schema_version VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            schema_content TEXT,
            UNIQUE(table_name, schema_hash)
        )
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(tracking_table_ddl))
                conn.commit()
            logger.info("Schema tracking table ensured")
        except Exception as e:
            logger.error(f"Failed to create schema tracking table: {e}")
            raise

    def calculate_schema_hash(self, schema_content: str) -> str:
        """Calcular hash del contenido del schema para detección de cambios"""
        return hashlib.sha256(schema_content.encode('utf-8')).hexdigest()

    def get_current_schema_version(self, table_name: str) -> Optional[SchemaVersion]:
        """Obtener versión actual del schema para una tabla"""
        schema_versions_table = self.schema_mapper.get_metadata_table_name("schema_versions")
        query = f"""
        SELECT table_name, schema_hash, schema_version, created_at, schema_content
        FROM {schema_versions_table} 
        WHERE table_name = :table_name 
        ORDER BY created_at DESC 
        LIMIT 1
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name}).fetchone()
                if result:
                    return SchemaVersion(
                        table_name=result[0],
                        schema_hash=result[1],
                        schema_version=result[2],
                        created_at=result[3],
                        schema_content=result[4]
                    )
            return None
        except Exception as e:
            logger.error(f"Failed to get schema version for {table_name}: {e}")
            return None

    def save_schema_version(self, table_name: str, schema_hash: str, 
                           schema_version: str, schema_content: str):
        """Guardar información de versión del schema"""
        schema_versions_table = self.schema_mapper.get_metadata_table_name("schema_versions")
        query = f"""
        INSERT INTO {schema_versions_table} (table_name, schema_hash, schema_version, schema_content)
        VALUES (:table_name, :schema_hash, :schema_version, :schema_content)
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query), {
                    "table_name": table_name,
                    "schema_hash": schema_hash,
                    "schema_version": schema_version,
                    "schema_content": schema_content
                })
                conn.commit()
            logger.info(f"Saved schema version {schema_version} for table {table_name}")
        except Exception as e:
            logger.error(f"Failed to save schema version: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Verificar si la tabla existe en la base de datos"""
        try:
            metadata = MetaData()
            metadata.reflect(bind=self.engine)
            return table_name in metadata.tables
        except Exception as e:
            logger.error(f"Failed to check if table {table_name} exists: {e}")
            return False

    def create_table_from_schema(self, table_name: str, schema_dict: Dict, schema_version: str = "1.0.0") -> bool:
        """Crear tabla desde schema JSON"""
        try:
            # Obtener nombre de tabla cualificado con esquema
            qualified_table_name = self.get_qualified_table_name(table_name, "data_schema")
            
            # Calcular hash del schema
            schema_hash = self.calculate_schema_hash(json.dumps(schema_dict, sort_keys=True))
            
            # Verificar si la tabla existe y el schema ha cambiado
            current_version = self.get_current_schema_version(qualified_table_name)
            if current_version and current_version.schema_hash == schema_hash:
                # Aunque el schema coincida, aseguramos la constraint UNIQUE/PK para ON CONFLICT
                try:
                    mapper = SchemaMapper(self.config.engine_type)
                    columns_defs = mapper.schema_to_columns(schema_dict)
                    pk_cols = [c.name for c in columns_defs if c.primary_key]
                    if pk_cols:
                        self._ensure_unique_constraint_for_keys(qualified_table_name, pk_cols)
                except Exception as e:
                    logger.warning(f"No se pudo asegurar constraint única para {qualified_table_name}: {e}")
                logger.info(f"Tabla {qualified_table_name} ya existe con el mismo schema")
                return True
            
            # Generar DDL usando SchemaMapper
            mapper = SchemaMapper(self.config.engine_type)
            ddl = mapper.schema_to_ddl(schema_dict, qualified_table_name, schema_version)
            
            # Ejecutar DDL
            with self.engine.connect() as conn:
                conn.execute(text(ddl))
                conn.commit()

            # Asegurar constraint UNIQUE/PK para claves detectadas (soporte ON CONFLICT)
            try:
                columns_defs = mapper.schema_to_columns(schema_dict)
                pk_cols = [c.name for c in columns_defs if c.primary_key]
                if pk_cols:
                    self._ensure_unique_constraint_for_keys(qualified_table_name, pk_cols)
            except Exception as e:
                logger.warning(f"No se pudo asegurar constraint única para {qualified_table_name}: {e}")

            # Guardar versión del schema
            self.save_schema_version(qualified_table_name, schema_hash, schema_version, json.dumps(schema_dict))
            
            logger.info(f"Tabla {qualified_table_name} creada exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error al crear tabla {qualified_table_name}: {e}")
            return False

    def write_dataframe(self, df: DataFrame, table_name: str, mode: str = "append", 
                       upsert_keys: Optional[List[str]] = None) -> bool:
        """Escribir DataFrame de Spark a tabla de base de datos con soporte para UPSERT"""
        try:
            # Si se especifican claves de upsert y el modo es append, usar lógica de upsert
            if upsert_keys and mode == "append":
                return self.write_dataframe_upsert(df, table_name, upsert_keys)
            
            # Comportamiento original para otros casos
            qualified_table_name = self.get_qualified_table_name(table_name, "data_schema")
            
            # Get database connection properties for Spark
            connection_props = self._get_spark_connection_properties()
            
            # Write DataFrame to database using JDBC connection string
            df.write \
                .format("jdbc") \
                .option("url", self.config.get_jdbc_connection_string()) \
                .option("dbtable", qualified_table_name) \
                .option("user", self.config.username) \
                .option("password", self.config.password) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()
            
            logger.info(f"DataFrame written to table {qualified_table_name} in {mode} mode")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write DataFrame to table {table_name}: {e}")
            return False

    def write_dataframe_upsert(self, df: DataFrame, table_name: str, upsert_keys: List[str]) -> bool:
        """Escribir DataFrame usando lógica UPSERT (INSERT ... ON CONFLICT para PostgreSQL)"""
        try:
            qualified_table_name = self.get_qualified_table_name(table_name, "data_schema")
            # Asegurar constraint UNIQUE/PK para las claves de upsert en la tabla destino
            try:
                self._ensure_unique_constraint_for_keys(qualified_table_name, upsert_keys)
            except Exception as e:
                logger.warning(f"No se pudo asegurar constraint única para {qualified_table_name}: {e}")
            
            # Crear tabla temporal para los nuevos datos
            temp_table_name = f"{qualified_table_name}_temp_{int(datetime.now().timestamp())}"
            
            # Normalizar nombres de columnas a minúsculas para evitar problemas de case-sensitivity en PostgreSQL
            # Esto asegura que columnas como 'IVA' se manejen como 'iva' en la tabla temporal y en el UPSERT
            df_temp = df.toDF(*[c.lower() for c in df.columns])

            # Escribir datos a tabla temporal
            df_temp.write \
                .format("jdbc") \
                .option("url", self.config.get_jdbc_connection_string()) \
                .option("dbtable", temp_table_name) \
                .option("user", self.config.username) \
                .option("password", self.config.password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
            
            # Obtener columnas del DataFrame
            df_columns = df_temp.columns
            
            # Construir query UPSERT para PostgreSQL
            upsert_query = self._build_upsert_query(
                target_table=qualified_table_name,
                temp_table=temp_table_name,
                columns=df_columns,
                upsert_keys=upsert_keys
            )
            
            # Ejecutar UPSERT
            with self.engine.connect() as conn:
                conn.execute(text(upsert_query))
                conn.commit()
                
                # Limpiar tabla temporal
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
                conn.commit()
            
            logger.info(f"DataFrame upserted to table {qualified_table_name} using keys: {upsert_keys}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert DataFrame to table {table_name}: {e}")
            # Intentar limpiar tabla temporal en caso de error
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
                    conn.commit()
            except:
                pass
            return False

    def _build_upsert_query(self, target_table: str, temp_table: str, 
                           columns: List[str], upsert_keys: List[str]) -> str:
        """Construir query UPSERT para PostgreSQL usando INSERT ... ON CONFLICT"""
        
        # Columnas para INSERT
        columns_str = ", ".join(columns)
        
        # Columnas para SELECT desde tabla temporal
        select_columns = ", ".join([f"temp.{col}" for col in columns])
        
        # Claves de conflicto
        conflict_keys = ", ".join(upsert_keys)
        
        # Columnas para UPDATE (excluir las claves de upsert)
        update_columns = [col for col in columns if col not in upsert_keys]
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        # Construir query completa
        if update_set:  # Si hay columnas para actualizar
            upsert_query = f"""
            INSERT INTO {target_table} ({columns_str})
            SELECT {select_columns}
            FROM {temp_table} temp
            ON CONFLICT ({conflict_keys})
            DO UPDATE SET {update_set}
            """
        else:  # Solo claves primarias, usar DO NOTHING
            upsert_query = f"""
            INSERT INTO {target_table} ({columns_str})
            SELECT {select_columns}
            FROM {temp_table} temp
            ON CONFLICT ({conflict_keys})
            DO NOTHING
            """
        
        return upsert_query

    def _get_spark_connection_properties(self) -> Dict[str, str]:
        """Obtener propiedades de conexión para JDBC de Spark con PostgreSQL"""
        return {
            "user": self.config.username,
            "password": self.config.password,
            "driver": "org.postgresql.Driver"
        }

    def _ensure_unique_constraint_for_keys(self, qualified_table_name: str, keys: List[str]):
        """Agregar constraint UNIQUE para claves si no existe, necesario para ON CONFLICT.

        Nota: Si la tabla ya tiene PRIMARY KEY sobre las mismas columnas, no se hace nada.
        """
        try:
            # Separar esquema y nombre de tabla
            if "." in qualified_table_name:
                schema_name, table_name = qualified_table_name.split(".", 1)
            else:
                schema_name, table_name = "public", qualified_table_name
            cols_list = ", ".join(keys)

            # Verificar si existe PK/UNIQUE para las columnas
            check_query = f"""
            SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = :schema
              AND tc.table_name = :table
              AND tc.constraint_type IN ('PRIMARY KEY','UNIQUE')
            """
            params = {"schema": schema_name, "table": table_name}

            with self.engine.connect() as conn:
                rows = conn.execute(text(check_query), params).fetchall()
                constraints = {}
                for name, ctype, col in rows:
                    constraints.setdefault(name, {"type": ctype, "cols": set()})
                    constraints[name]["cols"].add(col)
                keys_set = set(keys)
                # Debe existir una constraint EXACTA sobre las columnas indicadas.
                # Una UNIQUE compuesta (p.ej. payment_id, customer_id) NO satisface ON CONFLICT (payment_id).
                exists = any(keys_set == info["cols"] for info in constraints.values())
                if not exists:
                    constraint_name = f"{table_name}_" + "_".join(keys) + "_uniq"
                    add_query = f"ALTER TABLE {qualified_table_name} ADD CONSTRAINT {constraint_name} UNIQUE ({cols_list})"
                    conn.execute(text(add_query))
                    conn.commit()
                    logger.info(f"Constraint UNIQUE agregado: {qualified_table_name} ({cols_list})")
        except Exception as e:
            # No interrumpir el flujo principal; loguear y continuar
            logger.warning(f"No se pudo agregar UNIQUE({cols_list}) en {qualified_table_name}: {e}")

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Tuple]:
        """Ejecutar consulta y retornar resultados"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchall()
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise

    def close(self):
        """Cerrar conexión a base de datos"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("Conexión a base de datos cerrada")
    
    def _ensure_default_schemas(self):
        """Crear esquemas por defecto si no existen"""
        try:
            metadata_schema = self.schema_mapper.get_default_schema("metadata_schema")
            data_schema = self.schema_mapper.get_default_schema("data_schema")
            
            # Crear esquema de metadata
            if metadata_schema and metadata_schema != "public":
                self._create_schema_if_not_exists(metadata_schema)
            
            # Crear esquema de datos
            if data_schema and data_schema != "public":
                self._create_schema_if_not_exists(data_schema)
                
        except Exception as e:
            logger.error(f"Error al crear esquemas por defecto: {e}")
            # No lanzar excepción para no interrumpir la inicialización
    
    def _create_schema_if_not_exists(self, schema_name: str):
        """Crear esquema si no existe"""
        try:
            query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            logger.info(f"Esquema '{schema_name}' creado o ya existe")
        except Exception as e:
            logger.error(f"Error al crear esquema '{schema_name}': {e}")
            raise
    
    def get_qualified_table_name(self, table_name: str, schema_type: str = "data_schema") -> str:
        """Obtener nombre completo de tabla con esquema"""
        schema = self.schema_mapper.get_default_schema(schema_type)
        if schema and schema != "public":
            return f"{schema}.{table_name}"
        return table_name

    def log_pipeline_execution(self, dataset_name: str, pipeline_type: str = "etl", 
                             status: str = "started", error_message: str = None,
                             execution_id: str = None) -> str:
        """Registrar ejecución del pipeline en metadata.pipeline_executions"""
        try:
            pipeline_executions_table = self.schema_mapper.get_metadata_table_name("pipeline_executions")
            
            if execution_id is None:
                execution_id = f"{dataset_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
            
            if status == "started":
                query = f"""
                INSERT INTO {pipeline_executions_table} 
                (execution_id, dataset_name, pipeline_type, status, started_at)
                VALUES (:execution_id, :dataset_name, :pipeline_type, :status, :started_at)
                """
                params = {
                    "execution_id": execution_id,
                    "dataset_name": dataset_name,
                    "pipeline_type": pipeline_type,
                    "status": status,
                    "started_at": datetime.utcnow()
                }
            else:
                query = f"""
                UPDATE {pipeline_executions_table} 
                SET status = :status, ended_at = :ended_at, error_message = :error_message
                WHERE execution_id = :execution_id
                """
                params = {
                    "execution_id": execution_id,
                    "status": status,
                    "ended_at": datetime.utcnow(),
                    "error_message": error_message
                }
            
            with self.engine.connect() as conn:
                conn.execute(text(query), params)
                conn.commit()
                
            logger.info(f"Pipeline execution logged: {execution_id} - {status}")
            return execution_id
            
        except Exception as e:
            logger.error(f"Failed to log pipeline execution: {e}")
            raise

    def log_dataset_version(self, dataset_name: str, version: str, schema_path: str = None,
                          record_count: int = None, file_size_bytes: int = None) -> bool:
        """Registrar versión del dataset en metadata.dataset_versions"""
        try:
            dataset_versions_table = self.schema_mapper.get_metadata_table_name("dataset_versions")
            query = f"""
            INSERT INTO {dataset_versions_table} 
            (dataset_name, version, schema_path, record_count, file_size_bytes, created_at)
            VALUES (:dataset_name, :version, :schema_path, :record_count, :file_size_bytes, :created_at)
            """
            params = {
                "dataset_name": dataset_name,
                "version": version,
                "schema_path": schema_path,
                "record_count": record_count,
                "file_size_bytes": file_size_bytes,
                "created_at": datetime.utcnow()
            }
            
            with self.engine.connect() as conn:
                conn.execute(text(query), params)
                conn.commit()
                
            logger.info(f"Dataset version logged: {dataset_name} v{version}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to log dataset version: {e}")
            return False

    def get_latest_dataset_version(self, dataset_name: str) -> Optional[str]:
        """Obtener la última versión registrada de un dataset"""
        try:
            dataset_versions_table = self.schema_mapper.get_metadata_table_name("dataset_versions")
            query = f"""
            SELECT version FROM {dataset_versions_table} 
            WHERE dataset_name = :dataset_name 
            ORDER BY created_at DESC 
            LIMIT 1
            """
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"dataset_name": dataset_name})
                row = result.fetchone()
                return row[0] if row else None
                
        except Exception as e:
            logger.error(f"Failed to get latest dataset version: {e}")
            return None

    def get_pipeline_execution_status(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Obtener estado de una ejecución del pipeline"""
        try:
            pipeline_executions_table = self.schema_mapper.get_metadata_table_name("pipeline_executions")
            query = f"""
            SELECT execution_id, dataset_name, pipeline_type, status, started_at, ended_at, error_message
            FROM {pipeline_executions_table} 
            WHERE execution_id = :execution_id
            """
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"execution_id": execution_id})
                row = result.fetchone()
                if row:
                    return {
                        "execution_id": row[0],
                        "dataset_name": row[1],
                        "pipeline_type": row[2],
                        "status": row[3],
                        "started_at": row[4],
                        "ended_at": row[5],
                        "error_message": row[6]
                    }
                return None
                
        except Exception as e:
            logger.error(f"Failed to get pipeline execution status: {e}")
            return None


def create_database_manager_from_config(config_dict: Dict[str, Any]) -> DatabaseManager:
    """Factory function to create DatabaseManager from configuration dictionary"""
    engine_type = DatabaseEngine(config_dict["engine"])
    
    # Only PostgreSQL is supported
    if engine_type != DatabaseEngine.POSTGRESQL:
        raise ValueError(f"Only PostgreSQL is supported, got: {engine_type}")
    
    # Check if config has nested 'connection' or direct properties
    if "connection" in config_dict:
        connection = config_dict["connection"]
        host = connection["host"]
        port = connection["port"]
        database = connection["database"]
        username = connection["username"]
        password = connection["password"]
    else:
        host = config_dict["host"]
        port = config_dict["port"]
        database = config_dict["database"]
        username = config_dict["username"]
        password = config_dict["password"]
        
    db_config = DatabaseConfig(
        engine_type=engine_type,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        schema=config_dict.get("schema"),
        connection_params=config_dict.get("connection_params", {})
    )
    
    return DatabaseManager(db_config)


def create_database_manager_from_file(config_path: str, environment: str = "default") -> DatabaseManager:
    """Crear DatabaseManager desde archivo de configuración YAML"""
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Get environment-specific configuration
    # First try under 'environments' key, then try direct key
    env_config = config.get("environments", {}).get(environment)
    if not env_config:
        env_config = config.get(environment)
    
    if not env_config:
        available_envs = list(config.get("environments", {}).keys()) + [k for k in config.keys() if k not in ["table_settings", "jdbc_drivers", "connection_pool"]]
        raise ValueError(f"Environment '{environment}' not found in configuration. Available environments: {available_envs}")
    
    # Expand environment variables in configuration
    env_config = _expand_env_vars(env_config)
    
    return create_database_manager_from_config(env_config)


def _expand_env_vars(config: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively expand environment variables in configuration"""
    if isinstance(config, dict):
        return {k: _expand_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_expand_env_vars(item) for item in config]
    elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
        env_var = config[2:-1]
        return os.getenv(env_var, config)
    else:
        return config


# Example usage and testing
def main():
    """Ejemplo de uso"""
    # Configuración de ejemplo
    config = DatabaseConfig(
        engine_type=DatabaseEngine.POSTGRESQL,
        host="localhost",
        port=5432,
        database="test_db",
        username="user",
        password="password"
    )
    
    try:
        db_manager = DatabaseManager(config)
        
        # Crear tabla desde schema
        success = db_manager.create_table_from_schema(
            table_name="payments_v1",
            schema_path="config/datasets/finanzas/payments_v1/schema.json",
            schema_version="1.0.0"
        )
        
        if success:
            print("Tabla creada exitosamente!")
        else:
            print("Error al crear tabla")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()