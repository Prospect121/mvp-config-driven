"""
Utilidades para integración con servicios de Azure.
Incluye gestión de secretos con Key Vault, autenticación y configuración.
"""

import os
import json
import asyncio
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging

try:
    from azure.identity import DefaultAzureCredential, ClientSecretCredential, ManagedIdentityCredential
    from azure.keyvault.secrets import SecretClient
    from azure.storage.blob import BlobServiceClient
    from azure.eventhub import EventHubProducerClient, EventData
    from azure.monitor.opentelemetry import configure_azure_monitor
    import opentelemetry.trace as trace
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    logging.warning("Azure SDK no disponible. Usando configuración local.")

from src.utils.logging import get_logger, log_execution_time

logger = get_logger(__name__)

@dataclass
class AzureConfig:
    """Configuración para servicios de Azure."""
    
    # Key Vault
    key_vault_url: Optional[str] = None
    
    # Storage Account
    storage_account_name: Optional[str] = None
    storage_account_url: Optional[str] = None
    
    # Event Hub
    event_hub_namespace: Optional[str] = None
    event_hub_name: Optional[str] = None
    
    # SQL Database
    sql_server: Optional[str] = None
    sql_database: Optional[str] = None
    
    # Authentication
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    use_managed_identity: bool = True
    
    # Monitoring
    application_insights_connection_string: Optional[str] = None
    
    # Local development overrides
    local_mode: bool = False
    local_config: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_environment(cls) -> 'AzureConfig':
        """Crear configuración desde variables de entorno."""
        return cls(
            key_vault_url=os.getenv('AZURE_KEY_VAULT_URL'),
            storage_account_name=os.getenv('AZURE_STORAGE_ACCOUNT_NAME'),
            storage_account_url=os.getenv('AZURE_STORAGE_ACCOUNT_URL'),
            event_hub_namespace=os.getenv('AZURE_EVENT_HUB_NAMESPACE'),
            event_hub_name=os.getenv('AZURE_EVENT_HUB_NAME'),
            sql_server=os.getenv('AZURE_SQL_SERVER'),
            sql_database=os.getenv('AZURE_SQL_DATABASE'),
            tenant_id=os.getenv('AZURE_TENANT_ID'),
            client_id=os.getenv('AZURE_CLIENT_ID'),
            client_secret=os.getenv('AZURE_CLIENT_SECRET'),
            use_managed_identity=os.getenv('AZURE_USE_MANAGED_IDENTITY', 'true').lower() == 'true',
            application_insights_connection_string=os.getenv('APPLICATIONINSIGHTS_CONNECTION_STRING'),
            local_mode=os.getenv('ENVIRONMENT', 'local') == 'local',
            local_config={
                'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
                'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
                'kafka_bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                'sql_connection_string': os.getenv('SQL_CONNECTION_STRING', 
                    'mssql+pyodbc://sa:YourStrong@Passw0rd@localhost:1433/mvp_config_driven?driver=ODBC+Driver+17+for+SQL+Server'),
                'redis_url': os.getenv('REDIS_URL', 'redis://localhost:6379')
            }
        )

class AzureCredentialManager:
    """Gestor de credenciales para Azure."""
    
    def __init__(self, config: AzureConfig):
        self.config = config
        self._credential = None
        
    def get_credential(self):
        """Obtener credencial de Azure."""
        if not AZURE_AVAILABLE:
            logger.warning("Azure SDK no disponible")
            return None
            
        if self._credential is None:
            if self.config.local_mode:
                logger.info("Modo local: usando DefaultAzureCredential")
                self._credential = DefaultAzureCredential()
            elif self.config.use_managed_identity:
                logger.info("Usando Managed Identity")
                self._credential = ManagedIdentityCredential()
            elif self.config.client_id and self.config.client_secret and self.config.tenant_id:
                logger.info("Usando Service Principal")
                self._credential = ClientSecretCredential(
                    tenant_id=self.config.tenant_id,
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret
                )
            else:
                logger.info("Usando DefaultAzureCredential")
                self._credential = DefaultAzureCredential()
                
        return self._credential

class SecretManager:
    """Gestor de secretos con Azure Key Vault y fallback local."""
    
    def __init__(self, config: AzureConfig):
        self.config = config
        self.credential_manager = AzureCredentialManager(config)
        self._secret_client = None
        self._local_secrets = {}
        
        # Cargar secretos locales si están disponibles
        self._load_local_secrets()
    
    def _load_local_secrets(self):
        """Cargar secretos desde archivo local para desarrollo."""
        local_secrets_file = os.path.join(os.getcwd(), '.env.secrets')
        if os.path.exists(local_secrets_file):
            try:
                with open(local_secrets_file, 'r') as f:
                    for line in f:
                        if '=' in line and not line.strip().startswith('#'):
                            key, value = line.strip().split('=', 1)
                            self._local_secrets[key] = value
                logger.info(f"Cargados {len(self._local_secrets)} secretos locales")
            except Exception as e:
                logger.warning(f"Error cargando secretos locales: {e}")
    
    def _get_secret_client(self) -> Optional[SecretClient]:
        """Obtener cliente de Key Vault."""
        if not AZURE_AVAILABLE or self.config.local_mode:
            return None
            
        if self._secret_client is None and self.config.key_vault_url:
            credential = self.credential_manager.get_credential()
            if credential:
                self._secret_client = SecretClient(
                    vault_url=self.config.key_vault_url,
                    credential=credential
                )
        
        return self._secret_client
    
    @log_execution_time
    async def get_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Obtener secreto de Key Vault o configuración local.
        
        Args:
            secret_name: Nombre del secreto
            default: Valor por defecto si no se encuentra
            
        Returns:
            Valor del secreto o default
        """
        try:
            # Intentar Key Vault primero
            secret_client = self._get_secret_client()
            if secret_client:
                try:
                    secret = secret_client.get_secret(secret_name)
                    logger.debug(f"Secreto '{secret_name}' obtenido de Key Vault")
                    return secret.value
                except Exception as e:
                    logger.warning(f"Error obteniendo secreto '{secret_name}' de Key Vault: {e}")
            
            # Fallback a secretos locales
            if secret_name in self._local_secrets:
                logger.debug(f"Secreto '{secret_name}' obtenido de configuración local")
                return self._local_secrets[secret_name]
            
            # Fallback a variables de entorno
            env_value = os.getenv(secret_name)
            if env_value:
                logger.debug(f"Secreto '{secret_name}' obtenido de variable de entorno")
                return env_value
            
            logger.warning(f"Secreto '{secret_name}' no encontrado")
            return default
            
        except Exception as e:
            logger.error(f"Error obteniendo secreto '{secret_name}': {e}")
            return default
    
    async def get_secrets(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        """Obtener múltiples secretos."""
        tasks = [self.get_secret(name) for name in secret_names]
        values = await asyncio.gather(*tasks, return_exceptions=True)
        
        result = {}
        for name, value in zip(secret_names, values):
            if isinstance(value, Exception):
                logger.error(f"Error obteniendo secreto '{name}': {value}")
                result[name] = None
            else:
                result[name] = value
        
        return result
    
    async def set_secret(self, secret_name: str, secret_value: str) -> bool:
        """
        Establecer secreto en Key Vault (solo en producción).
        
        Args:
            secret_name: Nombre del secreto
            secret_value: Valor del secreto
            
        Returns:
            True si se estableció correctamente
        """
        if self.config.local_mode:
            logger.warning("Modo local: no se pueden establecer secretos en Key Vault")
            return False
            
        try:
            secret_client = self._get_secret_client()
            if secret_client:
                secret_client.set_secret(secret_name, secret_value)
                logger.info(f"Secreto '{secret_name}' establecido en Key Vault")
                return True
            else:
                logger.error("Cliente de Key Vault no disponible")
                return False
                
        except Exception as e:
            logger.error(f"Error estableciendo secreto '{secret_name}': {e}")
            return False

class AzureStorageManager:
    """Gestor de Azure Storage con fallback a MinIO."""
    
    def __init__(self, config: AzureConfig, secret_manager: SecretManager):
        self.config = config
        self.secret_manager = secret_manager
        self.credential_manager = AzureCredentialManager(config)
        self._blob_client = None
    
    async def _get_blob_client(self) -> Optional[BlobServiceClient]:
        """Obtener cliente de Blob Storage."""
        if self._blob_client is None:
            if self.config.local_mode:
                # Usar MinIO en modo local
                logger.info("Modo local: usando MinIO")
                return None
            elif AZURE_AVAILABLE and self.config.storage_account_url:
                credential = self.credential_manager.get_credential()
                if credential:
                    self._blob_client = BlobServiceClient(
                        account_url=self.config.storage_account_url,
                        credential=credential
                    )
        
        return self._blob_client
    
    async def upload_blob(self, container_name: str, blob_name: str, data: Union[str, bytes]) -> bool:
        """Subir blob a Azure Storage o MinIO."""
        try:
            blob_client = await self._get_blob_client()
            if blob_client:
                blob_client = blob_client.get_blob_client(
                    container=container_name,
                    blob=blob_name
                )
                blob_client.upload_blob(data, overwrite=True)
                logger.info(f"Blob '{blob_name}' subido a contenedor '{container_name}'")
                return True
            else:
                logger.warning("Cliente de Storage no disponible, usando almacenamiento local")
                # Implementar fallback local si es necesario
                return False
                
        except Exception as e:
            logger.error(f"Error subiendo blob '{blob_name}': {e}")
            return False
    
    async def download_blob(self, container_name: str, blob_name: str) -> Optional[bytes]:
        """Descargar blob de Azure Storage o MinIO."""
        try:
            blob_client = await self._get_blob_client()
            if blob_client:
                blob_client = blob_client.get_blob_client(
                    container=container_name,
                    blob=blob_name
                )
                download_stream = blob_client.download_blob()
                return download_stream.readall()
            else:
                logger.warning("Cliente de Storage no disponible")
                return None
                
        except Exception as e:
            logger.error(f"Error descargando blob '{blob_name}': {e}")
            return None

class AzureEventHubManager:
    """Gestor de Azure Event Hub con fallback a Kafka."""
    
    def __init__(self, config: AzureConfig, secret_manager: SecretManager):
        self.config = config
        self.secret_manager = secret_manager
        self.credential_manager = AzureCredentialManager(config)
        self._producer_client = None
    
    async def _get_producer_client(self) -> Optional[EventHubProducerClient]:
        """Obtener cliente productor de Event Hub."""
        if self._producer_client is None:
            if self.config.local_mode:
                logger.info("Modo local: usar Kafka en lugar de Event Hub")
                return None
            elif AZURE_AVAILABLE and self.config.event_hub_namespace and self.config.event_hub_name:
                # Obtener connection string de Key Vault
                connection_string = await self.secret_manager.get_secret(
                    "event-hub-connection-string"
                )
                if connection_string:
                    self._producer_client = EventHubProducerClient.from_connection_string(
                        conn_str=connection_string,
                        eventhub_name=self.config.event_hub_name
                    )
        
        return self._producer_client
    
    async def send_events(self, events: List[Dict[str, Any]]) -> bool:
        """Enviar eventos a Event Hub o Kafka."""
        try:
            producer_client = await self._get_producer_client()
            if producer_client:
                event_data_batch = await producer_client.create_batch()
                
                for event in events:
                    event_data = EventData(json.dumps(event))
                    try:
                        event_data_batch.add(event_data)
                    except ValueError:
                        # Batch lleno, enviar y crear nuevo
                        await producer_client.send_batch(event_data_batch)
                        event_data_batch = await producer_client.create_batch()
                        event_data_batch.add(event_data)
                
                # Enviar batch final
                if len(event_data_batch) > 0:
                    await producer_client.send_batch(event_data_batch)
                
                logger.info(f"Enviados {len(events)} eventos a Event Hub")
                return True
            else:
                logger.warning("Cliente de Event Hub no disponible, usar Kafka local")
                # Implementar fallback a Kafka si es necesario
                return False
                
        except Exception as e:
            logger.error(f"Error enviando eventos: {e}")
            return False

class AzureMonitoringManager:
    """Gestor de monitoreo con Azure Application Insights."""
    
    def __init__(self, config: AzureConfig):
        self.config = config
        self._tracer = None
        self._configured = False
    
    def configure_monitoring(self):
        """Configurar monitoreo con Application Insights."""
        if (not self._configured and 
            AZURE_AVAILABLE and 
            self.config.application_insights_connection_string and
            not self.config.local_mode):
            
            try:
                configure_azure_monitor(
                    connection_string=self.config.application_insights_connection_string
                )
                self._tracer = trace.get_tracer(__name__)
                self._configured = True
                logger.info("Monitoreo de Azure configurado")
            except Exception as e:
                logger.warning(f"Error configurando monitoreo de Azure: {e}")
    
    def get_tracer(self):
        """Obtener tracer para telemetría."""
        if not self._configured:
            self.configure_monitoring()
        return self._tracer

class AzureServiceManager:
    """Gestor principal para todos los servicios de Azure."""
    
    def __init__(self, config: Optional[AzureConfig] = None):
        self.config = config or AzureConfig.from_environment()
        self.secret_manager = SecretManager(self.config)
        self.storage_manager = AzureStorageManager(self.config, self.secret_manager)
        self.event_hub_manager = AzureEventHubManager(self.config, self.secret_manager)
        self.monitoring_manager = AzureMonitoringManager(self.config)
        
        # Configurar monitoreo al inicializar
        self.monitoring_manager.configure_monitoring()
    
    async def initialize(self):
        """Inicializar servicios de Azure."""
        logger.info("Inicializando servicios de Azure...")
        
        # Verificar conectividad con Key Vault
        test_secret = await self.secret_manager.get_secret("test-connection", "default")
        if test_secret:
            logger.info("Conexión con Key Vault establecida")
        else:
            logger.warning("No se pudo conectar con Key Vault, usando configuración local")
        
        logger.info("Servicios de Azure inicializados")
    
    async def get_database_connection_string(self) -> str:
        """Obtener cadena de conexión de base de datos."""
        if self.config.local_mode:
            return self.config.local_config.get('sql_connection_string', '')
        
        # Obtener de Key Vault
        connection_string = await self.secret_manager.get_secret(
            "sql-connection-string",
            default=""
        )
        
        if not connection_string and self.config.sql_server and self.config.sql_database:
            # Construir connection string básico
            username = await self.secret_manager.get_secret("sql-username", "")
            password = await self.secret_manager.get_secret("sql-password", "")
            
            if username and password:
                connection_string = (
                    f"mssql+pyodbc://{username}:{password}@"
                    f"{self.config.sql_server}/{self.config.sql_database}"
                    f"?driver=ODBC+Driver+17+for+SQL+Server"
                )
        
        return connection_string
    
    async def get_storage_config(self) -> Dict[str, Any]:
        """Obtener configuración de almacenamiento."""
        if self.config.local_mode:
            return {
                'type': 'minio',
                'endpoint': self.config.local_config.get('minio_endpoint'),
                'access_key': self.config.local_config.get('minio_access_key'),
                'secret_key': self.config.local_config.get('minio_secret_key'),
                'secure': False
            }
        
        return {
            'type': 'azure_blob',
            'account_url': self.config.storage_account_url,
            'credential_type': 'managed_identity' if self.config.use_managed_identity else 'service_principal'
        }
    
    async def get_event_hub_config(self) -> Dict[str, Any]:
        """Obtener configuración de Event Hub."""
        if self.config.local_mode:
            return {
                'type': 'kafka',
                'bootstrap_servers': self.config.local_config.get('kafka_bootstrap_servers'),
                'topics': {
                    'telemetry': 'telemetry-events',
                    'transactions': 'transaction-events'
                }
            }
        
        connection_string = await self.secret_manager.get_secret("event-hub-connection-string")
        return {
            'type': 'event_hub',
            'connection_string': connection_string,
            'event_hub_name': self.config.event_hub_name
        }

# Instancia global del gestor de servicios
_azure_service_manager: Optional[AzureServiceManager] = None

def get_azure_service_manager() -> AzureServiceManager:
    """Obtener instancia global del gestor de servicios de Azure."""
    global _azure_service_manager
    if _azure_service_manager is None:
        _azure_service_manager = AzureServiceManager()
    return _azure_service_manager

async def initialize_azure_services():
    """Inicializar servicios de Azure."""
    manager = get_azure_service_manager()
    await manager.initialize()

# Funciones de conveniencia
async def get_secret(secret_name: str, default: Optional[str] = None) -> Optional[str]:
    """Función de conveniencia para obtener secretos."""
    manager = get_azure_service_manager()
    return await manager.secret_manager.get_secret(secret_name, default)

async def send_telemetry_events(events: List[Dict[str, Any]]) -> bool:
    """Función de conveniencia para enviar eventos de telemetría."""
    manager = get_azure_service_manager()
    return await manager.event_hub_manager.send_events(events)

def get_tracer():
    """Función de conveniencia para obtener tracer de telemetría."""
    manager = get_azure_service_manager()
    return manager.monitoring_manager.get_tracer()