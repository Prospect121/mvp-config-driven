#!/usr/bin/env python3
"""
Script para configurar recursos de Azure basado en archivos de configuración YAML
Archivo: scripts/configure_azure_resources.py
"""

import yaml
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.eventhub import EventHubManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.sql import SqlManagementClient
from azure.keyvault.secrets import SecretClient

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AzureResourceConfigurator:
    """Clase para configurar recursos de Azure basado en archivos YAML"""
    
    def __init__(self, subscription_id: str):
        """
        Inicializa el configurador con las credenciales de Azure
        
        Args:
            subscription_id: ID de la suscripción de Azure
        """
        self.subscription_id = subscription_id
        self.credential = DefaultAzureCredential()
        
        # Inicializar clientes de Azure
        self.df_client = DataFactoryManagementClient(
            self.credential, subscription_id
        )
        self.eh_client = EventHubManagementClient(
            self.credential, subscription_id
        )
        self.storage_client = StorageManagementClient(
            self.credential, subscription_id
        )
        self.sql_client = SqlManagementClient(
            self.credential, subscription_id
        )
        
        # Configuración del proyecto
        self.resource_group = "mvp-config-driven-pipeline-dev-rg"
        self.key_vault_url = "https://mvp-dev-kv-v2.vault.azure.net/"
        
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Carga configuración desde archivo YAML
        
        Args:
            config_path: Ruta al archivo de configuración
            
        Returns:
            Diccionario con la configuración
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuración cargada desde: {config_path}")
            return config
        except Exception as e:
            logger.error(f"Error cargando configuración: {e}")
            raise
    
    def get_secret_from_keyvault(self, secret_name: str) -> str:
        """
        Obtiene un secreto desde Azure Key Vault
        
        Args:
            secret_name: Nombre del secreto
            
        Returns:
            Valor del secreto
        """
        try:
            secret_client = SecretClient(
                vault_url=self.key_vault_url,
                credential=self.credential
            )
            secret = secret_client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            logger.error(f"Error obteniendo secreto {secret_name}: {e}")
            raise
    
    def configure_data_factory(self, config_path: str) -> None:
        """
        Configura Azure Data Factory basado en archivo de configuración
        
        Args:
            config_path: Ruta al archivo de configuración de Data Factory
        """
        logger.info("Iniciando configuración de Data Factory...")
        
        config = self.load_config(config_path)
        df_config = config.get('data_factory', {})
        
        df_name = df_config.get('name')
        if not df_name:
            logger.error("Nombre de Data Factory no especificado en configuración")
            return
        
        try:
            # Configurar Linked Services
            self._configure_linked_services(
                df_name, 
                config.get('linked_services', {})
            )
            
            # Configurar Datasets
            self._configure_datasets(
                df_name, 
                config.get('datasets', {})
            )
            
            # Configurar Pipelines
            self._configure_pipelines(
                df_name, 
                config.get('pipelines', {})
            )
            
            # Configurar Triggers
            self._configure_triggers(
                df_name, 
                config.get('triggers', {})
            )
            
            logger.info("Configuración de Data Factory completada exitosamente")
            
        except Exception as e:
            logger.error(f"Error configurando Data Factory: {e}")
            raise
    
    def _configure_linked_services(self, df_name: str, linked_services: Dict[str, Any]) -> None:
        """Configura Linked Services en Data Factory"""
        logger.info("Configurando Linked Services...")
        
        for service_key, service_config in linked_services.items():
            try:
                service_name = service_config.get('name')
                service_type = service_config.get('type')
                
                if service_type == 'AzureBlobStorage':
                    connection_string = self.get_secret_from_keyvault(
                        service_config.get('connection_string_secret')
                    )
                    
                    linked_service = {
                        "type": "AzureBlobStorage",
                        "typeProperties": {
                            "connectionString": connection_string
                        }
                    }
                    
                elif service_type == 'AzureSqlDatabase':
                    connection_string = self.get_secret_from_keyvault(
                        service_config.get('connection_string_secret')
                    )
                    
                    linked_service = {
                        "type": "AzureSqlDatabase",
                        "typeProperties": {
                            "connectionString": connection_string
                        }
                    }
                
                elif service_type == 'AzureEventHubs':
                    connection_string = self.get_secret_from_keyvault(
                        service_config.get('connection_string_secret')
                    )
                    
                    linked_service = {
                        "type": "AzureEventHubs",
                        "typeProperties": {
                            "connectionString": connection_string
                        }
                    }
                
                # Crear o actualizar Linked Service
                self.df_client.linked_services.create_or_update(
                    resource_group_name=self.resource_group,
                    factory_name=df_name,
                    linked_service_name=service_name,
                    linked_service=linked_service
                )
                
                logger.info(f"Linked Service configurado: {service_name}")
                
            except Exception as e:
                logger.error(f"Error configurando Linked Service {service_key}: {e}")
    
    def _configure_datasets(self, df_name: str, datasets: Dict[str, Any]) -> None:
        """Configura Datasets en Data Factory"""
        logger.info("Configurando Datasets...")
        
        for dataset_key, dataset_config in datasets.items():
            try:
                dataset_name = dataset_config.get('name')
                dataset_type = dataset_config.get('type')
                linked_service = dataset_config.get('linked_service')
                properties = dataset_config.get('properties', {})
                
                if dataset_type == 'DelimitedText':
                    dataset = {
                        "type": "DelimitedText",
                        "linkedServiceName": {
                            "referenceName": linked_service,
                            "type": "LinkedServiceReference"
                        },
                        "typeProperties": {
                            "location": properties.get('location', {}),
                            "columnDelimiter": properties.get('format', {}).get('column_delimiter', ','),
                            "rowDelimiter": properties.get('format', {}).get('row_delimiter', '\\n'),
                            "firstRowAsHeader": properties.get('format', {}).get('first_row_as_header', True)
                        }
                    }
                
                elif dataset_type == 'AzureSqlTable':
                    dataset = {
                        "type": "AzureSqlTable",
                        "linkedServiceName": {
                            "referenceName": linked_service,
                            "type": "LinkedServiceReference"
                        },
                        "typeProperties": {
                            "schema": properties.get('schema', 'dbo'),
                            "table": properties.get('table')
                        }
                    }
                
                # Crear o actualizar Dataset
                self.df_client.datasets.create_or_update(
                    resource_group_name=self.resource_group,
                    factory_name=df_name,
                    dataset_name=dataset_name,
                    dataset=dataset
                )
                
                logger.info(f"Dataset configurado: {dataset_name}")
                
            except Exception as e:
                logger.error(f"Error configurando Dataset {dataset_key}: {e}")
    
    def _configure_pipelines(self, df_name: str, pipelines: Dict[str, Any]) -> None:
        """Configura Pipelines en Data Factory"""
        logger.info("Configurando Pipelines...")
        
        for pipeline_key, pipeline_config in pipelines.items():
            try:
                pipeline_name = pipeline_config.get('name')
                description = pipeline_config.get('description', '')
                activities = pipeline_config.get('activities', [])
                
                pipeline_activities = []
                for activity in activities:
                    if activity.get('type') == 'Copy':
                        copy_activity = {
                            "name": activity.get('name'),
                            "type": "Copy",
                            "inputs": [
                                {
                                    "referenceName": activity.get('source', {}).get('dataset'),
                                    "type": "DatasetReference"
                                }
                            ],
                            "outputs": [
                                {
                                    "referenceName": activity.get('sink', {}).get('dataset'),
                                    "type": "DatasetReference"
                                }
                            ],
                            "typeProperties": {
                                "source": {
                                    "type": activity.get('source', {}).get('type')
                                },
                                "sink": {
                                    "type": activity.get('sink', {}).get('type')
                                }
                            }
                        }
                        pipeline_activities.append(copy_activity)
                
                pipeline = {
                    "properties": {
                        "description": description,
                        "activities": pipeline_activities
                    }
                }
                
                # Crear o actualizar Pipeline
                self.df_client.pipelines.create_or_update(
                    resource_group_name=self.resource_group,
                    factory_name=df_name,
                    pipeline_name=pipeline_name,
                    pipeline=pipeline
                )
                
                logger.info(f"Pipeline configurado: {pipeline_name}")
                
            except Exception as e:
                logger.error(f"Error configurando Pipeline {pipeline_key}: {e}")
    
    def _configure_triggers(self, df_name: str, triggers: Dict[str, Any]) -> None:
        """Configura Triggers en Data Factory"""
        logger.info("Configurando Triggers...")
        
        for trigger_key, trigger_config in triggers.items():
            try:
                trigger_name = trigger_config.get('name')
                trigger_type = trigger_config.get('type')
                pipeline = trigger_config.get('pipeline')
                
                if trigger_type == 'ScheduleTrigger':
                    schedule = trigger_config.get('schedule', {})
                    trigger = {
                        "type": "ScheduleTrigger",
                        "typeProperties": {
                            "recurrence": {
                                "frequency": schedule.get('frequency', 'Day'),
                                "interval": schedule.get('interval', 1),
                                "startTime": schedule.get('start_time'),
                                "timeZone": schedule.get('time_zone', 'UTC')
                            }
                        },
                        "pipelines": [
                            {
                                "pipelineReference": {
                                    "referenceName": pipeline,
                                    "type": "PipelineReference"
                                }
                            }
                        ]
                    }
                
                # Crear o actualizar Trigger
                self.df_client.triggers.create_or_update(
                    resource_group_name=self.resource_group,
                    factory_name=df_name,
                    trigger_name=trigger_name,
                    trigger=trigger
                )
                
                logger.info(f"Trigger configurado: {trigger_name}")
                
            except Exception as e:
                logger.error(f"Error configurando Trigger {trigger_key}: {e}")
    
    def configure_event_hub(self, config_path: str) -> None:
        """
        Configura Azure Event Hub basado en archivo de configuración
        
        Args:
            config_path: Ruta al archivo de configuración de Event Hub
        """
        logger.info("Iniciando configuración de Event Hub...")
        
        config = self.load_config(config_path)
        
        try:
            # Configurar Event Hubs individuales
            event_hubs = config.get('event_hubs', {})
            namespace_name = config.get('event_hub_namespace', {}).get('name')
            
            for eh_key, eh_config in event_hubs.items():
                self._configure_individual_event_hub(namespace_name, eh_config)
            
            logger.info("Configuración de Event Hub completada exitosamente")
            
        except Exception as e:
            logger.error(f"Error configurando Event Hub: {e}")
            raise
    
    def _configure_individual_event_hub(self, namespace_name: str, eh_config: Dict[str, Any]) -> None:
        """Configura un Event Hub individual"""
        try:
            eh_name = eh_config.get('name')
            partition_count = eh_config.get('partition_count', 4)
            retention_days = eh_config.get('message_retention_days', 7)
            
            # Configurar Event Hub
            event_hub = {
                "partition_count": partition_count,
                "message_retention_in_days": retention_days,
                "status": eh_config.get('status', 'Active')
            }
            
            self.eh_client.event_hubs.create_or_update(
                resource_group_name=self.resource_group,
                namespace_name=namespace_name,
                event_hub_name=eh_name,
                parameters=event_hub
            )
            
            # Configurar Consumer Groups
            consumer_groups = eh_config.get('consumer_groups', [])
            for cg in consumer_groups:
                self.eh_client.consumer_groups.create_or_update(
                    resource_group_name=self.resource_group,
                    namespace_name=namespace_name,
                    event_hub_name=eh_name,
                    consumer_group_name=cg.get('name'),
                    parameters={
                        "user_metadata": cg.get('user_metadata', '')
                    }
                )
            
            logger.info(f"Event Hub configurado: {eh_name}")
            
        except Exception as e:
            logger.error(f"Error configurando Event Hub individual: {e}")
    
    def test_configuration(self) -> Dict[str, bool]:
        """
        Prueba la configuración de los recursos
        
        Returns:
            Diccionario con el estado de cada recurso
        """
        logger.info("Iniciando pruebas de configuración...")
        
        results = {}
        
        try:
            # Probar Data Factory
            df_list = self.df_client.factories.list_by_resource_group(
                self.resource_group
            )
            results['data_factory'] = len(list(df_list)) > 0
            
            # Probar Event Hub
            eh_list = self.eh_client.namespaces.list_by_resource_group(
                self.resource_group
            )
            results['event_hub'] = len(list(eh_list)) > 0
            
            # Probar Storage Account
            storage_list = self.storage_client.storage_accounts.list_by_resource_group(
                self.resource_group
            )
            results['storage_account'] = len(list(storage_list)) > 0
            
            logger.info(f"Resultados de pruebas: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error en pruebas de configuración: {e}")
            return {}

def main():
    """Función principal del script"""
    
    # Configuración
    SUBSCRIPTION_ID = "d6a71f50-d4ae-463a-9b56-e4a54988c47e"
    CONFIG_DIR = Path(__file__).parent.parent / "config"
    
    # Inicializar configurador
    configurator = AzureResourceConfigurator(SUBSCRIPTION_ID)
    
    try:
        # Configurar Data Factory
        df_config_path = CONFIG_DIR / "data_factory_config.yml"
        if df_config_path.exists():
            configurator.configure_data_factory(str(df_config_path))
        else:
            logger.warning(f"Archivo de configuración no encontrado: {df_config_path}")
        
        # Configurar Event Hub
        eh_config_path = CONFIG_DIR / "eventhub_config.yml"
        if eh_config_path.exists():
            configurator.configure_event_hub(str(eh_config_path))
        else:
            logger.warning(f"Archivo de configuración no encontrado: {eh_config_path}")
        
        # Ejecutar pruebas
        test_results = configurator.test_configuration()
        
        # Mostrar resumen
        logger.info("=== RESUMEN DE CONFIGURACIÓN ===")
        for resource, status in test_results.items():
            status_text = "✅ OK" if status else "❌ ERROR"
            logger.info(f"{resource}: {status_text}")
        
        logger.info("Configuración completada exitosamente")
        
    except Exception as e:
        logger.error(f"Error en configuración: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())