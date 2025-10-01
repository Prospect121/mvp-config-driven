"""
Gestión centralizada de configuración para el pipeline de datos.
Soporta tanto desarrollo local como despliegue en Azure.
"""
import os
import yaml
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

@dataclass
class StorageConfig:
    """Configuración de almacenamiento (local o Azure)"""
    type: str  # 'local', 'azure_adls', 'azure_blob'
    connection_string: Optional[str] = None
    account_name: Optional[str] = None
    account_key: Optional[str] = None
    sas_token: Optional[str] = None
    container_name: Optional[str] = None
    endpoint_url: Optional[str] = None  # Para MinIO local

@dataclass
class DatabaseConfig:
    """Configuración de base de datos (local o Azure SQL)"""
    type: str  # 'sqlite', 'postgresql', 'sqlserver'
    host: Optional[str] = None
    port: Optional[int] = None
    database: str = ""
    username: Optional[str] = None
    password: Optional[str] = None
    connection_string: Optional[str] = None

@dataclass
class EventHubConfig:
    """Configuración de Event Hubs (o Kafka local)"""
    type: str  # 'kafka', 'eventhub'
    connection_string: Optional[str] = None
    bootstrap_servers: Optional[str] = None
    topic_name: str = ""
    consumer_group: str = "default"

@dataclass
class KeyVaultConfig:
    """Configuración de Azure Key Vault"""
    vault_url: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    tenant_id: Optional[str] = None

@dataclass
class SecurityConfig:
    """Configuración de seguridad"""
    enable_tls: bool = True
    min_tls_version: str = "1.2"
    encrypt_at_rest: bool = True
    use_managed_identity: bool = False

class ConfigManager:
    """Gestor centralizado de configuración"""
    
    def __init__(self, config_path: str = None, env_path: str = None):
        self.config_path = config_path or os.getenv('CONFIG_PATH', 'config/env.yml')
        self.env_path = env_path or os.getenv('ENV_PATH', 'config/env/local.yml')
        self.environment = os.getenv('ENVIRONMENT', 'local')
        
        self._config = self._load_config()
        self._env_config = self._load_env_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Carga configuración principal"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.warning(f"Config file not found: {self.config_path}")
            return {}
    
    def _load_env_config(self) -> Dict[str, Any]:
        """Carga configuración específica del entorno"""
        try:
            with open(self.env_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.warning(f"Environment config file not found: {self.env_path}")
            return {}
    
    def get_storage_config(self) -> StorageConfig:
        """Obtiene configuración de almacenamiento"""
        if self.environment == 'local':
            return StorageConfig(
                type='local',
                endpoint_url=os.getenv('S3A_ENDPOINT', 'http://localhost:9000'),
                account_name=os.getenv('MINIO_ROOT_USER', 'minio'),
                account_key=os.getenv('MINIO_ROOT_PASSWORD', 'minio12345')
            )
        else:
            return StorageConfig(
                type='azure_adls',
                account_name=os.getenv('AZURE_STORAGE_ACCOUNT'),
                account_key=os.getenv('AZURE_STORAGE_KEY'),
                connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING')
            )
    
    def get_database_config(self) -> DatabaseConfig:
        """Obtiene configuración de base de datos"""
        if self.environment == 'local':
            return DatabaseConfig(
                type='postgresql',
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', '5432')),
                database=os.getenv('DB_NAME', 'pipeline_db'),
                username=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', 'postgres')
            )
        else:
            return DatabaseConfig(
                type='sqlserver',
                connection_string=os.getenv('AZURE_SQL_CONNECTION_STRING')
            )
    
    def get_eventhub_config(self) -> EventHubConfig:
        """Obtiene configuración de Event Hubs/Kafka"""
        if self.environment == 'local':
            return EventHubConfig(
                type='kafka',
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                topic_name=os.getenv('KAFKA_TOPIC', 'data-events')
            )
        else:
            return EventHubConfig(
                type='eventhub',
                connection_string=os.getenv('EVENTHUB_CONNECTION_STRING'),
                topic_name=os.getenv('EVENTHUB_NAME', 'data-events')
            )
    
    def get_keyvault_config(self) -> KeyVaultConfig:
        """Obtiene configuración de Key Vault"""
        return KeyVaultConfig(
            vault_url=os.getenv('AZURE_KEYVAULT_URL'),
            client_id=os.getenv('AZURE_CLIENT_ID'),
            client_secret=os.getenv('AZURE_CLIENT_SECRET'),
            tenant_id=os.getenv('AZURE_TENANT_ID')
        )
    
    def get_security_config(self) -> SecurityConfig:
        """Obtiene configuración de seguridad"""
        return SecurityConfig(
            enable_tls=os.getenv('ENABLE_TLS', 'true').lower() == 'true',
            min_tls_version=os.getenv('MIN_TLS_VERSION', '1.2'),
            encrypt_at_rest=os.getenv('ENCRYPT_AT_REST', 'true').lower() == 'true',
            use_managed_identity=os.getenv('USE_MANAGED_IDENTITY', 'false').lower() == 'true'
        )
    
    def get_timezone(self) -> str:
        """Obtiene zona horaria configurada"""
        return self._env_config.get('timezone', os.getenv('TZ', 'UTC'))
    
    def get_fs_path(self, path_type: str = 'raw') -> str:
        """Obtiene path del filesystem según el entorno"""
        if self.environment == 'local':
            return f"s3a://{path_type}/"
        else:
            storage_account = os.getenv('AZURE_STORAGE_ACCOUNT')
            return f"abfss://{path_type}@{storage_account}.dfs.core.windows.net/"
    
    def resolve_path_template(self, template: str, **kwargs) -> str:
        """Resuelve plantillas de path con variables"""
        resolved = template.format(
            fs=self.get_fs_path(),
            **kwargs
        )
        return resolved

# Instancia global del gestor de configuración
config_manager = ConfigManager()