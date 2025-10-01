"""
Implementación del patrón Observer para manejo de eventos y notificaciones
en el pipeline de datos. Permite desacoplar componentes y facilitar la
extensibilidad del sistema.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
import json

from ..utils.logging import get_logger, set_correlation_id

logger = get_logger(__name__)

class EventType(Enum):
    """Tipos de eventos del sistema."""
    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_COMPLETED = "pipeline_completed"
    PIPELINE_FAILED = "pipeline_failed"
    STEP_STARTED = "step_started"
    STEP_COMPLETED = "step_completed"
    STEP_FAILED = "step_failed"
    DATA_QUALITY_CHECK = "data_quality_check"
    DATA_VALIDATION_FAILED = "data_validation_failed"
    THRESHOLD_EXCEEDED = "threshold_exceeded"
    SYSTEM_ERROR = "system_error"
    CUSTOM_EVENT = "custom_event"

@dataclass
class Event:
    """Clase base para eventos del sistema."""
    event_type: EventType
    timestamp: datetime
    correlation_id: str
    source: str
    data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convierte el evento a diccionario."""
        return {
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "correlation_id": self.correlation_id,
            "source": self.source,
            "data": self.data,
            "metadata": self.metadata or {}
        }
    
    def to_json(self) -> str:
        """Convierte el evento a JSON."""
        return json.dumps(self.to_dict(), default=str, ensure_ascii=False)

class Observer(ABC):
    """Interfaz base para observadores."""
    
    @abstractmethod
    async def handle_event(self, event: Event) -> None:
        """Maneja un evento recibido."""
        pass
    
    @abstractmethod
    def get_supported_events(self) -> List[EventType]:
        """Retorna los tipos de eventos que este observador puede manejar."""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Nombre del observador."""
        pass

class Subject:
    """Sujeto observable que notifica eventos a los observadores."""
    
    def __init__(self, name: str):
        self.name = name
        self._observers: List[Observer] = []
        self._event_filters: Dict[Observer, List[EventType]] = {}
        self._executor = ThreadPoolExecutor(max_workers=5)
        self._lock = threading.Lock()
    
    def attach(self, observer: Observer, event_types: Optional[List[EventType]] = None) -> None:
        """Adjunta un observador para tipos específicos de eventos."""
        with self._lock:
            if observer not in self._observers:
                self._observers.append(observer)
                
            # Filtrar por tipos de eventos si se especifican
            if event_types:
                supported_events = observer.get_supported_events()
                filtered_events = [et for et in event_types if et in supported_events]
                self._event_filters[observer] = filtered_events
            else:
                self._event_filters[observer] = observer.get_supported_events()
                
        logger.info(
            f"Observador adjuntado: {observer.name}",
            observer=observer.name,
            event_types=[et.value for et in self._event_filters[observer]]
        )
    
    def detach(self, observer: Observer) -> None:
        """Desadjunta un observador."""
        with self._lock:
            if observer in self._observers:
                self._observers.remove(observer)
                self._event_filters.pop(observer, None)
                
        logger.info(f"Observador desadjuntado: {observer.name}", observer=observer.name)
    
    async def notify(self, event: Event) -> None:
        """Notifica un evento a todos los observadores relevantes."""
        logger.debug(
            f"Notificando evento: {event.event_type.value}",
            event_type=event.event_type.value,
            source=event.source,
            observers_count=len(self._observers)
        )
        
        # Obtener observadores que deben ser notificados
        relevant_observers = []
        with self._lock:
            for observer in self._observers:
                if event.event_type in self._event_filters.get(observer, []):
                    relevant_observers.append(observer)
        
        # Notificar a observadores de forma asíncrona
        if relevant_observers:
            tasks = []
            for observer in relevant_observers:
                task = asyncio.create_task(self._safe_notify_observer(observer, event))
                tasks.append(task)
            
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _safe_notify_observer(self, observer: Observer, event: Event) -> None:
        """Notifica a un observador de forma segura, manejando excepciones."""
        try:
            await observer.handle_event(event)
            logger.debug(
                f"Evento procesado por observador: {observer.name}",
                observer=observer.name,
                event_type=event.event_type.value
            )
        except Exception as e:
            logger.error(
                f"Error al notificar observador: {observer.name}",
                exception=e,
                observer=observer.name,
                event_type=event.event_type.value
            )

class EventBus:
    """Bus de eventos centralizado para toda la aplicación."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._subjects: Dict[str, Subject] = {}
            self._global_observers: List[Observer] = []
            self._initialized = True
    
    def get_subject(self, name: str) -> Subject:
        """Obtiene o crea un sujeto por nombre."""
        if name not in self._subjects:
            self._subjects[name] = Subject(name)
        return self._subjects[name]
    
    def register_global_observer(self, observer: Observer) -> None:
        """Registra un observador global que recibe todos los eventos."""
        self._global_observers.append(observer)
        
        # Adjuntar a todos los sujetos existentes
        for subject in self._subjects.values():
            subject.attach(observer)
    
    async def publish_event(self, subject_name: str, event: Event) -> None:
        """Publica un evento en un sujeto específico."""
        subject = self.get_subject(subject_name)
        await subject.notify(event)
        
        # También notificar a observadores globales
        for observer in self._global_observers:
            if event.event_type in observer.get_supported_events():
                await subject._safe_notify_observer(observer, event)

# Observadores específicos

class LoggingObserver(Observer):
    """Observador que registra eventos en logs."""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.LoggingObserver")
    
    async def handle_event(self, event: Event) -> None:
        """Registra el evento en logs."""
        if event.event_type in [EventType.PIPELINE_FAILED, EventType.STEP_FAILED, EventType.SYSTEM_ERROR]:
            self.logger.error(
                f"Evento de error: {event.event_type.value}",
                **event.data
            )
        elif event.event_type in [EventType.DATA_VALIDATION_FAILED, EventType.THRESHOLD_EXCEEDED]:
            self.logger.warning(
                f"Evento de advertencia: {event.event_type.value}",
                **event.data
            )
        else:
            self.logger.info(
                f"Evento: {event.event_type.value}",
                **event.data
            )
    
    def get_supported_events(self) -> List[EventType]:
        """Soporta todos los tipos de eventos."""
        return list(EventType)
    
    @property
    def name(self) -> str:
        return "LoggingObserver"

class MetricsObserver(Observer):
    """Observador que recolecta métricas de eventos."""
    
    def __init__(self):
        self.logger = get_logger(f"{__name__}.MetricsObserver")
        self.metrics: Dict[str, int] = {}
        self._lock = threading.Lock()
    
    async def handle_event(self, event: Event) -> None:
        """Recolecta métricas del evento."""
        with self._lock:
            metric_key = f"{event.source}.{event.event_type.value}"
            self.metrics[metric_key] = self.metrics.get(metric_key, 0) + 1
        
        # Log métricas específicas
        if event.event_type == EventType.PIPELINE_COMPLETED:
            duration = event.data.get('duration_ms', 0)
            self.logger.info(
                "Métrica de pipeline completado",
                metric_type="pipeline_duration",
                pipeline_name=event.data.get('pipeline_name'),
                duration_ms=duration
            )
        elif event.event_type == EventType.DATA_QUALITY_CHECK:
            self.logger.info(
                "Métrica de calidad de datos",
                metric_type="data_quality",
                **event.data
            )
    
    def get_metrics(self) -> Dict[str, int]:
        """Retorna las métricas recolectadas."""
        with self._lock:
            return self.metrics.copy()
    
    def get_supported_events(self) -> List[EventType]:
        """Soporta eventos de métricas."""
        return [
            EventType.PIPELINE_STARTED,
            EventType.PIPELINE_COMPLETED,
            EventType.PIPELINE_FAILED,
            EventType.STEP_COMPLETED,
            EventType.DATA_QUALITY_CHECK
        ]
    
    @property
    def name(self) -> str:
        return "MetricsObserver"

class NotificationObserver(Observer):
    """Observador que envía notificaciones para eventos críticos."""
    
    def __init__(self, notification_service=None):
        self.logger = get_logger(f"{__name__}.NotificationObserver")
        self.notification_service = notification_service
    
    async def handle_event(self, event: Event) -> None:
        """Envía notificaciones para eventos críticos."""
        if self._is_critical_event(event):
            await self._send_notification(event)
    
    def _is_critical_event(self, event: Event) -> bool:
        """Determina si un evento es crítico."""
        critical_events = [
            EventType.PIPELINE_FAILED,
            EventType.SYSTEM_ERROR,
            EventType.DATA_VALIDATION_FAILED,
            EventType.THRESHOLD_EXCEEDED
        ]
        return event.event_type in critical_events
    
    async def _send_notification(self, event: Event) -> None:
        """Envía una notificación."""
        try:
            if self.notification_service:
                await self.notification_service.send_alert(event)
            else:
                # Log como fallback
                self.logger.critical(
                    f"ALERTA CRÍTICA: {event.event_type.value}",
                    **event.data
                )
        except Exception as e:
            self.logger.error(
                "Error al enviar notificación",
                exception=e,
                event_type=event.event_type.value
            )
    
    def get_supported_events(self) -> List[EventType]:
        """Soporta eventos críticos."""
        return [
            EventType.PIPELINE_FAILED,
            EventType.SYSTEM_ERROR,
            EventType.DATA_VALIDATION_FAILED,
            EventType.THRESHOLD_EXCEEDED
        ]
    
    @property
    def name(self) -> str:
        return "NotificationObserver"

# Funciones de utilidad

def create_event(event_type: EventType, 
                source: str, 
                data: Dict[str, Any],
                correlation_id: Optional[str] = None,
                metadata: Optional[Dict[str, Any]] = None) -> Event:
    """Crea un nuevo evento."""
    if correlation_id is None:
        correlation_id = set_correlation_id()
    
    return Event(
        event_type=event_type,
        timestamp=datetime.utcnow(),
        correlation_id=correlation_id,
        source=source,
        data=data,
        metadata=metadata
    )

async def publish_pipeline_event(event_type: EventType,
                                pipeline_name: str,
                                data: Dict[str, Any],
                                correlation_id: Optional[str] = None) -> None:
    """Publica un evento de pipeline."""
    event = create_event(
        event_type=event_type,
        source=f"pipeline.{pipeline_name}",
        data=data,
        correlation_id=correlation_id
    )
    
    event_bus = EventBus()
    await event_bus.publish_event("pipeline", event)

# Configuración por defecto
def setup_default_observers() -> EventBus:
    """Configura los observadores por defecto."""
    event_bus = EventBus()
    
    # Registrar observadores globales
    event_bus.register_global_observer(LoggingObserver())
    event_bus.register_global_observer(MetricsObserver())
    event_bus.register_global_observer(NotificationObserver())
    
    logger.info("Observadores por defecto configurados")
    return event_bus

# Instancia global del event bus
event_bus = setup_default_observers()

# Ejemplo de uso
if __name__ == "__main__":
    import asyncio
    
    async def ejemplo_uso():
        # Crear y publicar eventos
        await publish_pipeline_event(
            EventType.PIPELINE_STARTED,
            "data_ingestion",
            {"pipeline_id": "123", "source": "api"}
        )
        
        await publish_pipeline_event(
            EventType.PIPELINE_COMPLETED,
            "data_ingestion",
            {"pipeline_id": "123", "duration_ms": 5000, "records_processed": 1000}
        )
    
    asyncio.run(ejemplo_uso())