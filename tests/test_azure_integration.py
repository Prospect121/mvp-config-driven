"""
Pruebas unitarias para la integración con Azure.
"""

import pytest
import os
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

from src.utils.azure_integration import (
    AzureConfig, AzureCredentialManager, SecretManager,
    AzureStorageManager, AzureEventHubManager, AzureMonitoringManager,
    AzureServiceManager, get_azure_service_manager, initialize_azure_services,
    get_secret, send_telemetry_events, get_tracer
)

class TestAzureConfig:
    """Pruebas para AzureConfig."""
    
    def test_config_creation_with_defaults(self):
        """Prueba creación de configuración con valores por defecto."""
        config = AzureConfig()
        
        # Verificar valores por defecto
        assert config.key_vault_url is None
        assert config.storage_account_name is None
        assert config.event_hub_namespace is None
        assert config.sql_server is None
        assert config.application_insights_key is None
        assert config.is_local_development is True
    
    def test_config_creation_with_values(self):
        """Prueba creación de configuración con valores específicos."""
        config = AzureConfig(
            key_vault_url="https://test-kv.vault.azure.net/",
            storage_account_name="teststorage",
            event_hub_namespace="test-eventhub",
            sql_server="test-sql.database.windows.net",
            sql_database="testdb",
            application_insights_key="test-insights-key",
            is_local_development=False
        )
        
        assert config.key_vault_url == "https://test-kv.vault.azure.net/"
        assert config.storage_account_name == "teststorage"
        assert config.event_hub_namespace == "test-eventhub"
        assert config.sql_server == "test-sql.database.windows.net"
        assert config.sql_database == "testdb"
        assert config.application_insights_key == "test-insights-key"
        assert config.is_local_development is False
    
    @patch.dict(os.environ, {
        'AZURE_KEY_VAULT_URL': 'https://env-kv.vault.azure.net/',
        'AZURE_STORAGE_ACCOUNT_NAME': 'envstorage',
        'AZURE_EVENT_HUB_NAMESPACE': 'env-eventhub',
        'AZURE_SQL_SERVER': 'env-sql.database.windows.net',
        'AZURE_SQL_DATABASE': 'envdb',
        'AZURE_APPLICATION_INSIGHTS_KEY': 'env-insights-key'
    })
    def test_config_from_environment(self):
        """Prueba carga de configuración desde variables de entorno."""
        config = AzureConfig.from_environment()
        
        assert config.key_vault_url == "https://env-kv.vault.azure.net/"
        assert config.storage_account_name == "envstorage"
        assert config.event_hub_namespace == "env-eventhub"
        assert config.sql_server == "env-sql.database.windows.net"
        assert config.sql_database == "envdb"
        assert config.application_insights_key == "env-insights-key"
    
    @patch.dict(os.environ, {}, clear=True)
    def test_config_local_development_detection(self):
        """Prueba detección automática de desarrollo local."""
        config = AzureConfig.from_environment()
        assert config.is_local_development is True
    
    @patch.dict(os.environ, {'AZURE_KEY_VAULT_URL': 'https://prod-kv.vault.azure.net/'})
    def test_config_production_detection(self):
        """Prueba detección de entorno de producción."""
        config = AzureConfig.from_environment()
        assert config.is_local_development is False

class TestAzureCredentialManager:
    """Pruebas para AzureCredentialManager."""
    
    def test_credential_manager_creation(self):
        """Prueba creación del gestor de credenciales."""
        manager = AzureCredentialManager()
        assert manager is not None
    
    @patch('src.utils.azure_integration.DefaultAzureCredential')
    def test_get_credential_default(self, mock_default_credential):
        """Prueba obtención de credencial por defecto."""
        mock_credential = MagicMock()
        mock_default_credential.return_value = mock_credential
        
        manager = AzureCredentialManager()
        credential = manager.get_credential()
        
        assert credential == mock_credential
        mock_default_credential.assert_called_once()
    
    @patch('src.utils.azure_integration.ManagedIdentityCredential')
    def test_get_credential_managed_identity(self, mock_managed_credential):
        """Prueba obtención de credencial de identidad administrada."""
        mock_credential = MagicMock()
        mock_managed_credential.return_value = mock_credential
        
        manager = AzureCredentialManager()
        credential = manager.get_credential(use_managed_identity=True)
        
        assert credential == mock_credential
        mock_managed_credential.assert_called_once()
    
    @patch('src.utils.azure_integration.ClientSecretCredential')
    def test_get_credential_service_principal(self, mock_client_credential):
        """Prueba obtención de credencial de service principal."""
        mock_credential = MagicMock()
        mock_client_credential.return_value = mock_credential
        
        manager = AzureCredentialManager()
        credential = manager.get_credential(
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret"
        )
        
        assert credential == mock_credential
        mock_client_credential.assert_called_once_with(
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret"
        )

class TestSecretManager:
    """Pruebas para SecretManager."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.config = AzureConfig(
            key_vault_url="https://test-kv.vault.azure.net/",
            is_local_development=False
        )
        self.credential_manager = MagicMock()
        self.secret_manager = SecretManager(self.config, self.credential_manager)
    
    @patch('src.utils.azure_integration.SecretClient')
    def test_get_secret_from_keyvault(self, mock_secret_client):
        """Prueba obtención de secreto desde Key Vault."""
        mock_client = MagicMock()
        mock_secret_client.return_value = mock_client
        
        mock_secret = MagicMock()
        mock_secret.value = "secret_value"
        mock_client.get_secret.return_value = mock_secret
        
        secret_value = self.secret_manager.get_secret("test-secret")
        
        assert secret_value == "secret_value"
        mock_client.get_secret.assert_called_once_with("test-secret")
    
    @patch('src.utils.azure_integration.SecretClient')
    def test_get_secret_not_found(self, mock_secret_client):
        """Prueba obtención de secreto no encontrado."""
        mock_client = MagicMock()
        mock_secret_client.return_value = mock_client
        
        from azure.core.exceptions import ResourceNotFoundError
        mock_client.get_secret.side_effect = ResourceNotFoundError("Secret not found")
        
        secret_value = self.secret_manager.get_secret("nonexistent-secret")
        assert secret_value is None
    
    @patch.dict(os.environ, {'TEST_SECRET': 'env_secret_value'})
    def test_get_secret_local_development(self):
        """Prueba obtención de secreto en desarrollo local."""
        local_config = AzureConfig(is_local_development=True)
        local_secret_manager = SecretManager(local_config, self.credential_manager)
        
        secret_value = local_secret_manager.get_secret("TEST_SECRET")
        assert secret_value == "env_secret_value"
    
    @patch('builtins.open', create=True)
    @patch('os.path.exists')
    def test_get_secret_from_file(self, mock_exists, mock_open):
        """Prueba obtención de secreto desde archivo local."""
        local_config = AzureConfig(is_local_development=True)
        local_secret_manager = SecretManager(local_config, self.credential_manager)
        
        mock_exists.return_value = True
        mock_file_content = "TEST_SECRET=file_secret_value\nOTHER_SECRET=other_value"
        mock_open.return_value.__enter__.return_value.read.return_value = mock_file_content
        
        secret_value = local_secret_manager.get_secret("TEST_SECRET")
        assert secret_value == "file_secret_value"

class TestAzureStorageManager:
    """Pruebas para AzureStorageManager."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.config = AzureConfig(
            storage_account_name="teststorage",
            is_local_development=False
        )
        self.credential_manager = MagicMock()
        self.storage_manager = AzureStorageManager(self.config, self.credential_manager)
    
    @patch('src.utils.azure_integration.BlobServiceClient')
    def test_upload_blob(self, mock_blob_service):
        """Prueba subida de blob."""
        mock_client = MagicMock()
        mock_blob_service.return_value = mock_client
        
        mock_blob_client = MagicMock()
        mock_client.get_blob_client.return_value = mock_blob_client
        
        data = b"test data"
        result = self.storage_manager.upload_blob("test-container", "test-blob", data)
        
        assert result is True
        mock_client.get_blob_client.assert_called_once_with(
            container="test-container", blob="test-blob"
        )
        mock_blob_client.upload_blob.assert_called_once_with(data, overwrite=True)
    
    @patch('src.utils.azure_integration.BlobServiceClient')
    def test_download_blob(self, mock_blob_service):
        """Prueba descarga de blob."""
        mock_client = MagicMock()
        mock_blob_service.return_value = mock_client
        
        mock_blob_client = MagicMock()
        mock_client.get_blob_client.return_value = mock_blob_client
        
        mock_download = MagicMock()
        mock_download.readall.return_value = b"downloaded data"
        mock_blob_client.download_blob.return_value = mock_download
        
        data = self.storage_manager.download_blob("test-container", "test-blob")
        
        assert data == b"downloaded data"
        mock_blob_client.download_blob.assert_called_once()
    
    @patch('src.utils.azure_integration.Minio')
    def test_upload_blob_local_development(self, mock_minio):
        """Prueba subida de blob en desarrollo local."""
        local_config = AzureConfig(is_local_development=True)
        local_storage_manager = AzureStorageManager(local_config, self.credential_manager)
        
        mock_client = MagicMock()
        mock_minio.return_value = mock_client
        
        data = b"test data"
        result = local_storage_manager.upload_blob("test-container", "test-blob", data)
        
        assert result is True
        mock_client.put_object.assert_called_once()

class TestAzureEventHubManager:
    """Pruebas para AzureEventHubManager."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.config = AzureConfig(
            event_hub_namespace="test-eventhub",
            is_local_development=False
        )
        self.credential_manager = MagicMock()
        self.event_hub_manager = AzureEventHubManager(self.config, self.credential_manager)
    
    @patch('src.utils.azure_integration.EventHubProducerClient')
    @pytest.mark.asyncio
    async def test_send_events(self, mock_producer_client):
        """Prueba envío de eventos."""
        mock_client = MagicMock()
        mock_producer_client.return_value = mock_client
        
        # Configurar el cliente como async context manager
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.send_batch = AsyncMock()
        
        events = [{"event": "test1"}, {"event": "test2"}]
        result = await self.event_hub_manager.send_events("test-hub", events)
        
        assert result is True
        mock_client.send_batch.assert_called_once()
    
    @patch('src.utils.azure_integration.KafkaProducer')
    def test_send_events_local_development(self, mock_kafka_producer):
        """Prueba envío de eventos en desarrollo local."""
        local_config = AzureConfig(is_local_development=True)
        local_event_hub_manager = AzureEventHubManager(local_config, self.credential_manager)
        
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer
        
        events = [{"event": "test1"}, {"event": "test2"}]
        
        # Para desarrollo local, el método es síncrono
        import asyncio
        result = asyncio.run(local_event_hub_manager.send_events("test-topic", events))
        
        assert result is True

class TestAzureMonitoringManager:
    """Pruebas para AzureMonitoringManager."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.config = AzureConfig(
            application_insights_key="test-insights-key",
            is_local_development=False
        )
        self.monitoring_manager = AzureMonitoringManager(self.config)
    
    @patch('src.utils.azure_integration.configure_azure_monitor')
    def test_configure_monitoring(self, mock_configure):
        """Prueba configuración de monitoreo."""
        self.monitoring_manager.configure_monitoring()
        mock_configure.assert_called_once_with(
            connection_string=f"InstrumentationKey={self.config.application_insights_key}"
        )
    
    def test_configure_monitoring_local_development(self):
        """Prueba configuración de monitoreo en desarrollo local."""
        local_config = AzureConfig(is_local_development=True)
        local_monitoring_manager = AzureMonitoringManager(local_config)
        
        # No debe lanzar excepción en desarrollo local
        local_monitoring_manager.configure_monitoring()
    
    @patch('src.utils.azure_integration.trace')
    def test_get_tracer(self, mock_trace):
        """Prueba obtención de tracer."""
        mock_tracer = MagicMock()
        mock_trace.get_tracer.return_value = mock_tracer
        
        tracer = self.monitoring_manager.get_tracer("test-service")
        
        assert tracer == mock_tracer
        mock_trace.get_tracer.assert_called_once_with("test-service")

class TestAzureServiceManager:
    """Pruebas para AzureServiceManager."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.config = AzureConfig(
            key_vault_url="https://test-kv.vault.azure.net/",
            storage_account_name="teststorage",
            event_hub_namespace="test-eventhub",
            sql_server="test-sql.database.windows.net",
            sql_database="testdb",
            application_insights_key="test-insights-key",
            is_local_development=False
        )
        self.service_manager = AzureServiceManager(self.config)
    
    def test_service_manager_initialization(self):
        """Prueba inicialización del gestor de servicios."""
        assert self.service_manager.config == self.config
        assert self.service_manager.credential_manager is not None
        assert self.service_manager.secret_manager is not None
        assert self.service_manager.storage_manager is not None
        assert self.service_manager.event_hub_manager is not None
        assert self.service_manager.monitoring_manager is not None
    
    @patch.object(AzureServiceManager, 'secret_manager')
    def test_get_database_connection_string(self, mock_secret_manager):
        """Prueba obtención de cadena de conexión de base de datos."""
        mock_secret_manager.get_secret.return_value = "test_password"
        
        connection_string = self.service_manager.get_database_connection_string("test_user")
        
        expected = (
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=tcp:test-sql.database.windows.net,1433;"
            "Database=testdb;"
            "Uid=test_user;"
            "Pwd=test_password;"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        assert connection_string == expected
    
    def test_get_database_connection_string_local(self):
        """Prueba obtención de cadena de conexión local."""
        local_config = AzureConfig(is_local_development=True)
        local_service_manager = AzureServiceManager(local_config)
        
        connection_string = local_service_manager.get_database_connection_string("test_user")
        
        assert "localhost,1433" in connection_string
        assert "mvp_config_driven" in connection_string
    
    def test_get_storage_config(self):
        """Prueba obtención de configuración de almacenamiento."""
        storage_config = self.service_manager.get_storage_config()
        
        assert storage_config["account_name"] == "teststorage"
        assert storage_config["is_local"] is False
    
    def test_get_storage_config_local(self):
        """Prueba obtención de configuración de almacenamiento local."""
        local_config = AzureConfig(is_local_development=True)
        local_service_manager = AzureServiceManager(local_config)
        
        storage_config = local_service_manager.get_storage_config()
        
        assert storage_config["endpoint"] == "localhost:9000"
        assert storage_config["is_local"] is True
    
    def test_get_event_hub_config(self):
        """Prueba obtención de configuración de Event Hub."""
        event_hub_config = self.service_manager.get_event_hub_config()
        
        assert event_hub_config["namespace"] == "test-eventhub"
        assert event_hub_config["is_local"] is False
    
    def test_get_event_hub_config_local(self):
        """Prueba obtención de configuración de Event Hub local."""
        local_config = AzureConfig(is_local_development=True)
        local_service_manager = AzureServiceManager(local_config)
        
        event_hub_config = local_service_manager.get_event_hub_config()
        
        assert event_hub_config["bootstrap_servers"] == "localhost:9092"
        assert event_hub_config["is_local"] is True

class TestGlobalFunctions:
    """Pruebas para funciones globales."""
    
    @patch('src.utils.azure_integration.AzureServiceManager')
    def test_get_azure_service_manager(self, mock_service_manager):
        """Prueba obtención del gestor de servicios global."""
        mock_manager = MagicMock()
        mock_service_manager.return_value = mock_manager
        
        manager1 = get_azure_service_manager()
        manager2 = get_azure_service_manager()
        
        # Debe retornar la misma instancia (singleton)
        assert manager1 is manager2
    
    @patch('src.utils.azure_integration.get_azure_service_manager')
    def test_initialize_azure_services(self, mock_get_manager):
        """Prueba inicialización de servicios Azure."""
        mock_manager = MagicMock()
        mock_get_manager.return_value = mock_manager
        
        initialize_azure_services()
        
        mock_manager.monitoring_manager.configure_monitoring.assert_called_once()
    
    @patch('src.utils.azure_integration.get_azure_service_manager')
    def test_get_secret(self, mock_get_manager):
        """Prueba obtención de secreto global."""
        mock_manager = MagicMock()
        mock_manager.secret_manager.get_secret.return_value = "secret_value"
        mock_get_manager.return_value = mock_manager
        
        secret = get_secret("test-secret")
        
        assert secret == "secret_value"
        mock_manager.secret_manager.get_secret.assert_called_once_with("test-secret")
    
    @patch('src.utils.azure_integration.get_azure_service_manager')
    @pytest.mark.asyncio
    async def test_send_telemetry_events(self, mock_get_manager):
        """Prueba envío de eventos de telemetría global."""
        mock_manager = MagicMock()
        mock_manager.event_hub_manager.send_events = AsyncMock(return_value=True)
        mock_get_manager.return_value = mock_manager
        
        events = [{"event": "test"}]
        result = await send_telemetry_events("test-hub", events)
        
        assert result is True
        mock_manager.event_hub_manager.send_events.assert_called_once_with("test-hub", events)
    
    @patch('src.utils.azure_integration.get_azure_service_manager')
    def test_get_tracer(self, mock_get_manager):
        """Prueba obtención de tracer global."""
        mock_manager = MagicMock()
        mock_tracer = MagicMock()
        mock_manager.monitoring_manager.get_tracer.return_value = mock_tracer
        mock_get_manager.return_value = mock_manager
        
        tracer = get_tracer("test-service")
        
        assert tracer == mock_tracer
        mock_manager.monitoring_manager.get_tracer.assert_called_once_with("test-service")

class TestIntegrationScenarios:
    """Pruebas de escenarios de integración."""
    
    @patch.dict(os.environ, {
        'AZURE_KEY_VAULT_URL': 'https://test-kv.vault.azure.net/',
        'AZURE_STORAGE_ACCOUNT_NAME': 'teststorage',
        'AZURE_EVENT_HUB_NAMESPACE': 'test-eventhub'
    })
    def test_production_environment_setup(self):
        """Prueba configuración de entorno de producción."""
        config = AzureConfig.from_environment()
        service_manager = AzureServiceManager(config)
        
        assert not config.is_local_development
        assert service_manager.config.key_vault_url == "https://test-kv.vault.azure.net/"
        assert service_manager.config.storage_account_name == "teststorage"
        assert service_manager.config.event_hub_namespace == "test-eventhub"
    
    @patch.dict(os.environ, {}, clear=True)
    def test_local_development_setup(self):
        """Prueba configuración de desarrollo local."""
        config = AzureConfig.from_environment()
        service_manager = AzureServiceManager(config)
        
        assert config.is_local_development
        
        # Verificar configuraciones locales
        storage_config = service_manager.get_storage_config()
        assert storage_config["is_local"] is True
        
        event_hub_config = service_manager.get_event_hub_config()
        assert event_hub_config["is_local"] is True
    
    @patch('src.utils.azure_integration.SecretClient')
    @patch('src.utils.azure_integration.BlobServiceClient')
    def test_azure_services_integration(self, mock_blob_service, mock_secret_client):
        """Prueba integración completa de servicios Azure."""
        config = AzureConfig(
            key_vault_url="https://test-kv.vault.azure.net/",
            storage_account_name="teststorage",
            is_local_development=False
        )
        service_manager = AzureServiceManager(config)
        
        # Configurar mocks
        mock_secret = MagicMock()
        mock_secret.value = "test_secret_value"
        mock_secret_client.return_value.get_secret.return_value = mock_secret
        
        mock_blob_client = MagicMock()
        mock_blob_service.return_value.get_blob_client.return_value = mock_blob_client
        
        # Probar obtención de secreto
        secret = service_manager.secret_manager.get_secret("test-secret")
        assert secret == "test_secret_value"
        
        # Probar subida de blob
        result = service_manager.storage_manager.upload_blob(
            "test-container", "test-blob", b"test data"
        )
        assert result is True

if __name__ == "__main__":
    pytest.main([__file__, "-v"])