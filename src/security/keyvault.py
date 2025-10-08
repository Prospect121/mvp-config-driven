"""
Integración con Azure Key Vault para gestión segura de secretos.
Incluye fallback para desarrollo local usando variables de entorno.
"""
import os
import logging
from typing import Optional, Dict, Any
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.core.exceptions import AzureError

logger = logging.getLogger(__name__)

class SecretManager:
    """Gestor de secretos que soporta Azure Key Vault y variables de entorno locales"""
    
    def __init__(self, vault_url: Optional[str] = None, 
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 tenant_id: Optional[str] = None):
        self.vault_url = vault_url or os.getenv('AZURE_KEYVAULT_URL')
        self.environment = os.getenv('ENVIRONMENT', 'local')
        self.client = None
        
        if self.environment != 'local' and self.vault_url:
            try:
                # Intentar usar Managed Identity primero, luego Service Principal
                if os.getenv('USE_MANAGED_IDENTITY', 'false').lower() == 'true':
                    credential = DefaultAzureCredential()
                else:
                    credential = ClientSecretCredential(
                        tenant_id=tenant_id or os.getenv('AZURE_TENANT_ID'),
                        client_id=client_id or os.getenv('AZURE_CLIENT_ID'),
                        client_secret=client_secret or os.getenv('AZURE_CLIENT_SECRET')
                    )
                
                self.client = SecretClient(vault_url=self.vault_url, credential=credential)
                logger.info("Azure Key Vault client initialized successfully")
                
            except Exception as e:
                logger.warning(f"Failed to initialize Key Vault client: {e}")
                logger.info("Falling back to environment variables")
    
    def get_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Obtiene un secreto desde Key Vault o variables de entorno.
        
        Args:
            secret_name: Nombre del secreto
            default: Valor por defecto si no se encuentra
            
        Returns:
            Valor del secreto o None si no se encuentra
        """
        # En desarrollo local, usar variables de entorno
        if self.environment == 'local' or not self.client:
            env_var = secret_name.upper().replace('-', '_')
            return os.getenv(env_var, default)
        
        try:
            secret = self.client.get_secret(secret_name)
            return secret.value
        except AzureError as e:
            logger.warning(f"Failed to retrieve secret '{secret_name}' from Key Vault: {e}")
            # Fallback a variable de entorno
            env_var = secret_name.upper().replace('-', '_')
            return os.getenv(env_var, default)
    
    def get_connection_string(self, service_name: str) -> Optional[str]:
        """
        Obtiene cadena de conexión para un servicio específico.
        
        Args:
            service_name: Nombre del servicio (sql-database, storage-account, eventhub)
            
        Returns:
            Cadena de conexión o None si no se encuentra
        """
        secret_name = f"{service_name}-connection-string"
        return self.get_secret(secret_name)
    
    def get_storage_credentials(self) -> Dict[str, Optional[str]]:
        """Obtiene credenciales de almacenamiento"""
        return {
            'account_name': self.get_secret('storage-account-name'),
            'account_key': self.get_secret('storage-account-key'),
            'connection_string': self.get_secret('storage-connection-string'),
            'sas_token': self.get_secret('storage-sas-token')
        }
    
    def get_database_credentials(self) -> Dict[str, Optional[str]]:
        """Obtiene credenciales de base de datos"""
        return {
            'username': self.get_secret('database-username'),
            'password': self.get_secret('database-password'),
            'connection_string': self.get_secret('database-connection-string')
        }
    
    def get_eventhub_credentials(self) -> Dict[str, Optional[str]]:
        """Obtiene credenciales de Event Hub"""
        return {
            'connection_string': self.get_secret('eventhub-connection-string'),
            'shared_access_key': self.get_secret('eventhub-shared-access-key')
        }
    
    def set_spark_configs(self, spark_session) -> None:
        """
        Configura Spark con las credenciales apropiadas según el entorno.
        
        Args:
            spark_session: Sesión de Spark a configurar
        """
        if self.environment == 'local':
            # Configuración para MinIO/S3A local
            try:
                storage_creds = self.get_storage_credentials()
                access_key = storage_creds.get('account_name', 'minio')
                secret_key = storage_creds.get('account_key', 'minio12345')
                
                # Solo configurar si tenemos credenciales válidas
                if access_key and secret_key:
                    spark_session.conf.set("spark.hadoop.fs.s3a.endpoint", 
                                         os.getenv('S3A_ENDPOINT', 'http://localhost:9000'))
                    spark_session.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
                    spark_session.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
                    spark_session.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
                    spark_session.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    logger.info("Configuración S3A aplicada para entorno local")
                else:
                    logger.warning("Credenciales S3A no disponibles, omitiendo configuración")
            except Exception as e:
                logger.warning(f"Error configurando S3A para entorno local: {e}")
        else:
            # Configuración para Azure Data Lake Storage Gen2
            storage_creds = self.get_storage_credentials()
            account_name = storage_creds.get('account_name')
            
            if account_name:
                if storage_creds.get('account_key'):
                    spark_session.conf.set(f"spark.hadoop.fs.azure.account.key.{account_name}.dfs.core.windows.net",
                                         storage_creds['account_key'])
                elif storage_creds.get('sas_token'):
                    spark_session.conf.set(f"spark.hadoop.fs.azure.sas.{account_name}.dfs.core.windows.net",
                                         storage_creds['sas_token'])
                
                # Habilitar TLS 1.2+
                spark_session.conf.set("spark.hadoop.fs.azure.secure.mode", "true")
                spark_session.conf.set("spark.hadoop.fs.azure.ssl.channel.mode", "Default_JSSE")

# Instancia global del gestor de secretos
secret_manager = SecretManager()