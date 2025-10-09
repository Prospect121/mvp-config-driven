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
        # PostgreSQL DDL for schema tracking
        tracking_table_ddl = """
        CREATE TABLE IF NOT EXISTS schema_versions (
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
        query = """
        SELECT table_name, schema_hash, schema_version, created_at, schema_content
        FROM schema_versions 
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
        query = """
        INSERT INTO schema_versions (table_name, schema_hash, schema_version, schema_content)
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
            # Calcular hash del schema
            schema_hash = self.calculate_schema_hash(json.dumps(schema_dict, sort_keys=True))
            
            # Verificar si la tabla existe y el schema ha cambiado
            current_version = self.get_current_schema_version(table_name)
            if current_version and current_version.schema_hash == schema_hash:
                logger.info(f"Tabla {table_name} ya existe con el mismo schema")
                return True
            
            # Generar DDL usando SchemaMapper
            mapper = SchemaMapper(self.config.engine_type)
            ddl = mapper.schema_to_ddl(schema_dict, table_name, schema_version)
            
            # Ejecutar DDL
            with self.engine.connect() as conn:
                conn.execute(text(ddl))
                conn.commit()
            
            # Guardar versión del schema
            self.save_schema_version(table_name, schema_hash, schema_version, json.dumps(schema_dict))
            
            logger.info(f"Tabla {table_name} creada exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error al crear tabla {table_name}: {e}")
            return False

    def write_dataframe(self, df: DataFrame, table_name: str, mode: str = "append") -> bool:
        """Escribir DataFrame de Spark a tabla de base de datos"""
        try:
            # Get database connection properties for Spark
            connection_props = self._get_spark_connection_properties()
            
            # Write DataFrame to database using JDBC connection string
            df.write \
                .format("jdbc") \
                .option("url", self.config.get_jdbc_connection_string()) \
                .option("dbtable", table_name) \
                .option("user", self.config.username) \
                .option("password", self.config.password) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()
            
            logger.info(f"DataFrame written to table {table_name} in {mode} mode")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write DataFrame to table {table_name}: {e}")
            return False

    def _get_spark_connection_properties(self) -> Dict[str, str]:
        """Obtener propiedades de conexión para JDBC de Spark con PostgreSQL"""
        return {
            "user": self.config.username,
            "password": self.config.password,
            "driver": "org.postgresql.Driver"
        }

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

    def log_pipeline_execution(self, dataset_name: str, pipeline_type: str = "etl", 
                             status: str = "started", error_message: str = None,
                             execution_id: str = None) -> str:
        """Registrar ejecución del pipeline en metadata.pipeline_executions"""
        try:
            if execution_id is None:
                execution_id = f"{dataset_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
            
            if status == "started":
                query = """
                INSERT INTO metadata.pipeline_executions 
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
                query = """
                UPDATE metadata.pipeline_executions 
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
            query = """
            INSERT INTO metadata.dataset_versions 
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
            query = """
            SELECT version FROM metadata.dataset_versions 
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
            query = """
            SELECT execution_id, dataset_name, pipeline_type, status, started_at, ended_at, error_message
            FROM metadata.pipeline_executions 
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