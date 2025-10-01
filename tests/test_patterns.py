"""
Pruebas unitarias para los patrones de diseño implementados.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
import pandas as pd

from src.patterns.observer import (
    Event, EventType, Observer, Subject, EventBus,
    LoggingObserver, MetricsObserver, NotificationObserver,
    create_event, publish_pipeline_event
)

from src.patterns.strategy import (
    ProcessingContext, DataFormat, ProcessingMode,
    DataProcessingStrategy, PandasBatchStrategy, StreamingStrategy,
    ProcessingStrategySelector, DataProcessor
)

class TestObserverPattern:
    """Pruebas para el patrón Observer."""
    
    def test_event_creation(self):
        """Prueba creación de eventos."""
        event = create_event(
            event_type=EventType.PIPELINE_STARTED,
            source="test_pipeline",
            data={"pipeline_id": "123", "user": "test_user"},
            correlation_id="test-correlation-123"
        )
        
        assert event.event_type == EventType.PIPELINE_STARTED
        assert event.source == "test_pipeline"
        assert event.correlation_id == "test-correlation-123"
        assert event.data["pipeline_id"] == "123"
        assert isinstance(event.timestamp, datetime)
    
    def test_event_serialization(self):
        """Prueba serialización de eventos."""
        event = create_event(
            event_type=EventType.PIPELINE_COMPLETED,
            source="test_pipeline",
            data={"duration": 5000, "records": 1000}
        )
        
        # Test to_dict
        event_dict = event.to_dict()
        assert event_dict["event_type"] == "pipeline_completed"
        assert event_dict["source"] == "test_pipeline"
        assert event_dict["data"]["duration"] == 5000
        
        # Test to_json
        event_json = event.to_json()
        assert isinstance(event_json, str)
        assert "pipeline_completed" in event_json

class MockObserver(Observer):
    """Observer mock para pruebas."""
    
    def __init__(self, name: str, supported_events: list = None):
        self._name = name
        self._supported_events = supported_events or list(EventType)
        self.received_events = []
    
    async def handle_event(self, event: Event) -> None:
        self.received_events.append(event)
    
    def get_supported_events(self) -> list:
        return self._supported_events
    
    @property
    def name(self) -> str:
        return self._name

class TestSubject:
    """Pruebas para la clase Subject."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.subject = Subject("test_subject")
        self.observer1 = MockObserver("observer1")
        self.observer2 = MockObserver("observer2", [EventType.PIPELINE_STARTED])
    
    def test_attach_observer(self):
        """Prueba adjuntar observador."""
        self.subject.attach(self.observer1)
        assert self.observer1 in self.subject._observers
        assert EventType.PIPELINE_STARTED in self.subject._event_filters[self.observer1]
    
    def test_attach_observer_with_filter(self):
        """Prueba adjuntar observador con filtro de eventos."""
        self.subject.attach(self.observer2, [EventType.PIPELINE_STARTED])
        assert self.observer2 in self.subject._observers
        assert self.subject._event_filters[self.observer2] == [EventType.PIPELINE_STARTED]
    
    def test_detach_observer(self):
        """Prueba desadjuntar observador."""
        self.subject.attach(self.observer1)
        self.subject.detach(self.observer1)
        assert self.observer1 not in self.subject._observers
        assert self.observer1 not in self.subject._event_filters
    
    @pytest.mark.asyncio
    async def test_notify_observers(self):
        """Prueba notificación a observadores."""
        self.subject.attach(self.observer1)
        self.subject.attach(self.observer2)
        
        event = create_event(
            event_type=EventType.PIPELINE_STARTED,
            source="test",
            data={"test": "data"}
        )
        
        await self.subject.notify(event)
        
        # Ambos observadores deben recibir el evento
        assert len(self.observer1.received_events) == 1
        assert len(self.observer2.received_events) == 1
        assert self.observer1.received_events[0] == event
    
    @pytest.mark.asyncio
    async def test_notify_filtered_observers(self):
        """Prueba notificación con filtros de eventos."""
        self.subject.attach(self.observer1)
        self.subject.attach(self.observer2, [EventType.PIPELINE_COMPLETED])
        
        # Evento que solo observer1 debe recibir
        event = create_event(
            event_type=EventType.PIPELINE_STARTED,
            source="test",
            data={"test": "data"}
        )
        
        await self.subject.notify(event)
        
        assert len(self.observer1.received_events) == 1
        assert len(self.observer2.received_events) == 0

class TestEventBus:
    """Pruebas para el EventBus."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        # Crear nueva instancia para cada prueba
        EventBus._instance = None
        self.event_bus = EventBus()
        self.observer = MockObserver("test_observer")
    
    def test_singleton_pattern(self):
        """Prueba que EventBus es singleton."""
        bus1 = EventBus()
        bus2 = EventBus()
        assert bus1 is bus2
    
    def test_get_subject(self):
        """Prueba obtención de sujetos."""
        subject1 = self.event_bus.get_subject("test_subject")
        subject2 = self.event_bus.get_subject("test_subject")
        
        assert subject1 is subject2
        assert subject1.name == "test_subject"
    
    def test_register_global_observer(self):
        """Prueba registro de observador global."""
        self.event_bus.register_global_observer(self.observer)
        assert self.observer in self.event_bus._global_observers
    
    @pytest.mark.asyncio
    async def test_publish_event(self):
        """Prueba publicación de eventos."""
        self.event_bus.register_global_observer(self.observer)
        
        event = create_event(
            event_type=EventType.PIPELINE_STARTED,
            source="test",
            data={"test": "data"}
        )
        
        await self.event_bus.publish_event("test_subject", event)
        
        # El observador global debe recibir el evento
        assert len(self.observer.received_events) >= 1

class TestBuiltInObservers:
    """Pruebas para observadores incorporados."""
    
    @pytest.mark.asyncio
    async def test_logging_observer(self):
        """Prueba LoggingObserver."""
        observer = LoggingObserver()
        
        event = create_event(
            event_type=EventType.PIPELINE_FAILED,
            source="test_pipeline",
            data={"error": "Test error", "pipeline_id": "123"}
        )
        
        # No debe lanzar excepción
        await observer.handle_event(event)
        
        # Verificar que soporta todos los eventos
        assert len(observer.get_supported_events()) == len(EventType)
    
    @pytest.mark.asyncio
    async def test_metrics_observer(self):
        """Prueba MetricsObserver."""
        observer = MetricsObserver()
        
        event = create_event(
            event_type=EventType.PIPELINE_COMPLETED,
            source="test_pipeline",
            data={"duration_ms": 5000, "pipeline_name": "test"}
        )
        
        await observer.handle_event(event)
        
        # Verificar que se recolectaron métricas
        metrics = observer.get_metrics()
        assert "test_pipeline.pipeline_completed" in metrics
        assert metrics["test_pipeline.pipeline_completed"] == 1
    
    @pytest.mark.asyncio
    async def test_notification_observer(self):
        """Prueba NotificationObserver."""
        mock_service = AsyncMock()
        observer = NotificationObserver(mock_service)
        
        # Evento crítico
        critical_event = create_event(
            event_type=EventType.PIPELINE_FAILED,
            source="test_pipeline",
            data={"error": "Critical error"}
        )
        
        await observer.handle_event(critical_event)
        
        # Debe llamar al servicio de notificación
        mock_service.send_alert.assert_called_once_with(critical_event)
        
        # Evento no crítico
        normal_event = create_event(
            event_type=EventType.PIPELINE_STARTED,
            source="test_pipeline",
            data={"pipeline_id": "123"}
        )
        
        mock_service.reset_mock()
        await observer.handle_event(normal_event)
        
        # No debe llamar al servicio
        mock_service.send_alert.assert_not_called()

class TestStrategyPattern:
    """Pruebas para el patrón Strategy."""
    
    def test_processing_context_creation(self):
        """Prueba creación de contexto de procesamiento."""
        context = ProcessingContext(
            data_format=DataFormat.CSV,
            processing_mode=ProcessingMode.BATCH,
            volume_estimate=10000,
            quality_requirements={"null_percentage": {"max_percentage": 5.0}},
            performance_requirements={"max_duration_minutes": 10}
        )
        
        assert context.data_format == DataFormat.CSV
        assert context.processing_mode == ProcessingMode.BATCH
        assert context.volume_estimate == 10000
        assert context.quality_requirements["null_percentage"]["max_percentage"] == 5.0

class MockStrategy(DataProcessingStrategy):
    """Estrategia mock para pruebas."""
    
    def __init__(self, name: str, formats: list, modes: list):
        self._name = name
        self._formats = formats
        self._modes = modes
        self.process_called = False
        self.process_result = "mock_result"
    
    async def process(self, data, context, config):
        self.process_called = True
        return self.process_result
    
    def supports_format(self, data_format):
        return data_format in self._formats
    
    def supports_mode(self, processing_mode):
        return processing_mode in self._modes
    
    def get_performance_characteristics(self):
        return {"mock": True}
    
    @property
    def name(self):
        return self._name

class TestPandasBatchStrategy:
    """Pruebas para PandasBatchStrategy."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.strategy = PandasBatchStrategy()
        self.context = ProcessingContext(
            data_format=DataFormat.CSV,
            processing_mode=ProcessingMode.BATCH,
            volume_estimate=1000,
            quality_requirements={
                "null_percentage": {"max_percentage": 5.0, "columns": ["amount"]},
                "duplicate_percentage": {"max_percentage": 1.0}
            },
            performance_requirements={}
        )
    
    def test_supports_format(self):
        """Prueba formatos soportados."""
        assert self.strategy.supports_format(DataFormat.CSV)
        assert self.strategy.supports_format(DataFormat.JSON)
        assert self.strategy.supports_format(DataFormat.PARQUET)
        assert not self.strategy.supports_format(DataFormat.AVRO)
    
    def test_supports_mode(self):
        """Prueba modos soportados."""
        assert self.strategy.supports_mode(ProcessingMode.BATCH)
        assert not self.strategy.supports_mode(ProcessingMode.STREAMING)
    
    @pytest.mark.asyncio
    async def test_process_dataframe(self):
        """Prueba procesamiento de DataFrame."""
        # Datos de prueba
        data = pd.DataFrame({
            'customer_id': [1, 2, 3, 4, 5],
            'amount': [100, 200, 300, 400, 500],
            'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
        })
        
        config = {
            'transformations': [
                {
                    'type': 'filter',
                    'condition': 'amount > 150'
                }
            ]
        }
        
        result = await self.strategy.process(data, self.context, config)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3  # Solo registros con amount > 150
        assert all(result['amount'] > 150)
    
    @pytest.mark.asyncio
    async def test_quality_validation_failure(self):
        """Prueba falla de validación de calidad."""
        # Datos con muchos nulos
        data = pd.DataFrame({
            'customer_id': [1, 2, 3, 4, 5],
            'amount': [100, None, None, None, None],  # 80% nulos
            'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
        })
        
        config = {'transformations': []}
        
        with pytest.raises(ValueError, match="excede el porcentaje máximo de nulos"):
            await self.strategy.process(data, self.context, config)

class TestStreamingStrategy:
    """Pruebas para StreamingStrategy."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.strategy = StreamingStrategy()
        self.context = ProcessingContext(
            data_format=DataFormat.JSON,
            processing_mode=ProcessingMode.STREAMING,
            volume_estimate=0,  # Streaming no tiene límite
            quality_requirements={},
            performance_requirements={}
        )
    
    def test_supports_format(self):
        """Prueba formatos soportados."""
        assert self.strategy.supports_format(DataFormat.JSON)
        assert self.strategy.supports_format(DataFormat.AVRO)
        assert not self.strategy.supports_format(DataFormat.CSV)
    
    def test_supports_mode(self):
        """Prueba modos soportados."""
        assert self.strategy.supports_mode(ProcessingMode.STREAMING)
        assert self.strategy.supports_mode(ProcessingMode.REAL_TIME)
        assert not self.strategy.supports_mode(ProcessingMode.BATCH)
    
    @pytest.mark.asyncio
    async def test_process_streaming_data(self):
        """Prueba procesamiento de datos streaming."""
        # Datos de prueba como lista (simulando stream)
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
            {"id": 4, "value": 40},
            {"id": 5, "value": 50}
        ]
        
        config = {
            'streaming': {'batch_size': 2},
            'transformations': [
                {
                    'type': 'filter',
                    'function': lambda x: x['value'] > 25
                }
            ]
        }
        
        result = await self.strategy.process(data, self.context, config)
        
        assert result["status"] == "completed"
        assert result["records_processed"] > 0

class TestProcessingStrategySelector:
    """Pruebas para el selector de estrategias."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.selector = ProcessingStrategySelector()
        
        # Registrar estrategias mock
        self.pandas_strategy = MockStrategy(
            "PandasStrategy", 
            [DataFormat.CSV, DataFormat.JSON], 
            [ProcessingMode.BATCH]
        )
        self.spark_strategy = MockStrategy(
            "SparkStrategy", 
            [DataFormat.CSV, DataFormat.PARQUET], 
            [ProcessingMode.BATCH, ProcessingMode.MICRO_BATCH]
        )
        self.streaming_strategy = MockStrategy(
            "StreamingStrategy", 
            [DataFormat.JSON], 
            [ProcessingMode.STREAMING]
        )
        
        self.selector.register_strategy(self.pandas_strategy)
        self.selector.register_strategy(self.spark_strategy)
        self.selector.register_strategy(self.streaming_strategy)
    
    def test_register_strategy(self):
        """Prueba registro de estrategias."""
        assert "PandasStrategy" in self.selector.strategies
        assert "SparkStrategy" in self.selector.strategies
        assert "StreamingStrategy" in self.selector.strategies
    
    def test_select_compatible_strategy(self):
        """Prueba selección de estrategia compatible."""
        context = ProcessingContext(
            data_format=DataFormat.JSON,
            processing_mode=ProcessingMode.STREAMING,
            volume_estimate=1000,
            quality_requirements={},
            performance_requirements={}
        )
        
        strategy = self.selector.select_strategy(context)
        assert strategy.name == "StreamingStrategy"
    
    def test_select_strategy_no_compatible(self):
        """Prueba selección sin estrategias compatibles."""
        context = ProcessingContext(
            data_format=DataFormat.XML,  # No soportado
            processing_mode=ProcessingMode.BATCH,
            volume_estimate=1000,
            quality_requirements={},
            performance_requirements={}
        )
        
        with pytest.raises(ValueError, match="No hay estrategias compatibles"):
            self.selector.select_strategy(context)
    
    def test_select_strategy_large_volume(self):
        """Prueba selección para volumen grande."""
        context = ProcessingContext(
            data_format=DataFormat.CSV,
            processing_mode=ProcessingMode.BATCH,
            volume_estimate=2_000_000,  # Volumen grande
            quality_requirements={},
            performance_requirements={}
        )
        
        strategy = self.selector.select_strategy(context)
        # Debe preferir Spark para volúmenes grandes
        assert "Spark" in strategy.name

class TestDataProcessor:
    """Pruebas para el procesador principal."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.processor = DataProcessor()
    
    @pytest.mark.asyncio
    async def test_process_data_with_pandas(self):
        """Prueba procesamiento con estrategia Pandas."""
        data = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10, 20, 30]
        })
        
        context = ProcessingContext(
            data_format=DataFormat.CSV,
            processing_mode=ProcessingMode.BATCH,
            volume_estimate=100,
            quality_requirements={},
            performance_requirements={}
        )
        
        config = {'transformations': []}
        
        result = await self.processor.process_data(data, context, config)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

if __name__ == "__main__":
    pytest.main([__file__, "-v"])