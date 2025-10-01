"""
Utilidad para logging estructurado con observabilidad y trazabilidad.
Proporciona logging consistente en formato JSON para mejor análisis y monitoreo.
"""

import json
import logging
import sys
import time
import uuid
from datetime import datetime
from functools import wraps
from typing import Any, Dict, Optional, Union
from contextvars import ContextVar
from dataclasses import dataclass, asdict
import traceback

# Context variables para trazabilidad
correlation_id: ContextVar[str] = ContextVar('correlation_id', default='')
user_id: ContextVar[str] = ContextVar('user_id', default='')
session_id: ContextVar[str] = ContextVar('session_id', default='')

@dataclass
class LogContext:
    """Contexto de logging para trazabilidad."""
    correlation_id: str = ""
    user_id: str = ""
    session_id: str = ""
    pipeline_id: str = ""
    step_id: str = ""
    
    def to_dict(self) -> Dict[str, str]:
        return {k: v for k, v in asdict(self).items() if v}

class StructuredFormatter(logging.Formatter):
    """Formateador para logs estructurados en JSON."""
    
    def __init__(self, service_name: str = "mvp-config-driven"):
        self.service_name = service_name
        super().__init__()
    
    def format(self, record: logging.LogRecord) -> str:
        """Formatea el log record como JSON estructurado."""
        
        # Información básica del log
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": self.service_name,
            "thread": record.thread,
            "process": record.process,
        }
        
        # Agregar contexto de trazabilidad
        context = LogContext(
            correlation_id=correlation_id.get(),
            user_id=user_id.get(),
            session_id=session_id.get()
        )
        
        if hasattr(record, 'pipeline_id'):
            context.pipeline_id = record.pipeline_id
        if hasattr(record, 'step_id'):
            context.step_id = record.step_id
            
        log_entry["context"] = context.to_dict()
        
        # Agregar información adicional del record
        if hasattr(record, 'extra_data'):
            log_entry["data"] = record.extra_data
            
        # Agregar información de excepción si existe
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Agregar métricas de performance si existen
        if hasattr(record, 'duration'):
            log_entry["performance"] = {
                "duration_ms": record.duration,
                "memory_usage_mb": getattr(record, 'memory_usage', None)
            }
            
        # Agregar información de ubicación del código
        log_entry["location"] = {
            "file": record.pathname,
            "function": record.funcName,
            "line": record.lineno
        }
        
        return json.dumps(log_entry, ensure_ascii=False, default=str)

class StructuredLogger:
    """Logger estructurado con funcionalidades avanzadas."""
    
    def __init__(self, name: str, service_name: str = "mvp-config-driven"):
        self.logger = logging.getLogger(name)
        self.service_name = service_name
        self._setup_logger()
    
    def _setup_logger(self):
        """Configura el logger con formato estructurado."""
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = StructuredFormatter(self.service_name)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _log(self, level: int, message: str, **kwargs):
        """Método interno para logging con datos adicionales."""
        extra = {}
        if kwargs:
            extra['extra_data'] = kwargs
        self.logger.log(level, message, extra=extra)
    
    def debug(self, message: str, **kwargs):
        """Log de debug con datos adicionales."""
        self._log(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log de información con datos adicionales."""
        self._log(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log de advertencia con datos adicionales."""
        self._log(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """Log de error con excepción opcional."""
        extra = {}
        if kwargs:
            extra['extra_data'] = kwargs
        if exception:
            self.logger.error(message, exc_info=exception, extra=extra)
        else:
            self.logger.error(message, extra=extra)
    
    def critical(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """Log crítico con excepción opcional."""
        extra = {}
        if kwargs:
            extra['extra_data'] = kwargs
        if exception:
            self.logger.critical(message, exc_info=exception, extra=extra)
        else:
            self.logger.critical(message, extra=extra)

def get_logger(name: str) -> StructuredLogger:
    """Factory function para obtener un logger estructurado."""
    return StructuredLogger(name)

def set_correlation_id(corr_id: Optional[str] = None) -> str:
    """Establece un correlation ID para trazabilidad."""
    if corr_id is None:
        corr_id = str(uuid.uuid4())
    correlation_id.set(corr_id)
    return corr_id

def set_user_context(user_id_val: str, session_id_val: Optional[str] = None):
    """Establece el contexto de usuario."""
    user_id.set(user_id_val)
    if session_id_val:
        session_id.set(session_id_val)

def log_execution_time(logger: Optional[StructuredLogger] = None):
    """Decorador para medir y loggear tiempo de ejecución."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            func_logger = logger or get_logger(func.__module__)
            
            try:
                func_logger.info(
                    f"Iniciando ejecución de {func.__name__}",
                    function=func.__name__,
                    args_count=len(args),
                    kwargs_keys=list(kwargs.keys())
                )
                
                result = func(*args, **kwargs)
                
                duration = (time.time() - start_time) * 1000
                func_logger.info(
                    f"Ejecución completada: {func.__name__}",
                    function=func.__name__,
                    duration_ms=duration,
                    success=True
                )
                
                return result
                
            except Exception as e:
                duration = (time.time() - start_time) * 1000
                func_logger.error(
                    f"Error en ejecución de {func.__name__}",
                    exception=e,
                    function=func.__name__,
                    duration_ms=duration,
                    success=False
                )
                raise
                
        return wrapper
    return decorator

def log_pipeline_step(step_name: str, pipeline_id: str):
    """Decorador para loggear pasos de pipeline."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            step_id = str(uuid.uuid4())
            func_logger = get_logger(func.__module__)
            
            # Agregar IDs al record
            old_factory = logging.getLogRecordFactory()
            
            def record_factory(*args, **kwargs):
                record = old_factory(*args, **kwargs)
                record.pipeline_id = pipeline_id
                record.step_id = step_id
                return record
            
            logging.setLogRecordFactory(record_factory)
            
            try:
                func_logger.info(
                    f"Iniciando paso de pipeline: {step_name}",
                    step_name=step_name,
                    pipeline_id=pipeline_id,
                    step_id=step_id
                )
                
                result = func(*args, **kwargs)
                
                func_logger.info(
                    f"Paso de pipeline completado: {step_name}",
                    step_name=step_name,
                    pipeline_id=pipeline_id,
                    step_id=step_id,
                    success=True
                )
                
                return result
                
            except Exception as e:
                func_logger.error(
                    f"Error en paso de pipeline: {step_name}",
                    exception=e,
                    step_name=step_name,
                    pipeline_id=pipeline_id,
                    step_id=step_id,
                    success=False
                )
                raise
            finally:
                logging.setLogRecordFactory(old_factory)
                
        return wrapper
    return decorator

class MetricsLogger:
    """Logger especializado para métricas de negocio."""
    
    def __init__(self, service_name: str = "mvp-config-driven"):
        self.logger = get_logger(f"{service_name}.metrics")
    
    def log_data_quality_metric(self, 
                               table_name: str,
                               metric_name: str, 
                               metric_value: Union[int, float],
                               threshold: Optional[Union[int, float]] = None,
                               passed: Optional[bool] = None):
        """Log de métricas de calidad de datos."""
        self.logger.info(
            f"Métrica de calidad de datos: {metric_name}",
            metric_type="data_quality",
            table_name=table_name,
            metric_name=metric_name,
            metric_value=metric_value,
            threshold=threshold,
            passed=passed
        )
    
    def log_pipeline_metric(self,
                           pipeline_name: str,
                           metric_name: str,
                           metric_value: Union[int, float],
                           unit: str = "count"):
        """Log de métricas de pipeline."""
        self.logger.info(
            f"Métrica de pipeline: {metric_name}",
            metric_type="pipeline",
            pipeline_name=pipeline_name,
            metric_name=metric_name,
            metric_value=metric_value,
            unit=unit
        )
    
    def log_performance_metric(self,
                              operation: str,
                              duration_ms: float,
                              records_processed: Optional[int] = None,
                              memory_usage_mb: Optional[float] = None):
        """Log de métricas de performance."""
        self.logger.info(
            f"Métrica de performance: {operation}",
            metric_type="performance",
            operation=operation,
            duration_ms=duration_ms,
            records_processed=records_processed,
            memory_usage_mb=memory_usage_mb,
            throughput_records_per_second=records_processed / (duration_ms / 1000) if records_processed and duration_ms > 0 else None
        )

# Instancia global de métricas
metrics_logger = MetricsLogger()

# Configuración de logging por defecto
def setup_logging(level: str = "INFO", service_name: str = "mvp-config-driven"):
    """Configura el logging global de la aplicación."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Configurar logger raíz con formato estructurado
    root_logger = logging.getLogger()
    if root_logger.handlers:
        for handler in root_logger.handlers:
            handler.setFormatter(StructuredFormatter(service_name))

# Ejemplo de uso
if __name__ == "__main__":
    # Configurar logging
    setup_logging("DEBUG")
    
    # Crear logger
    logger = get_logger(__name__)
    
    # Establecer contexto
    set_correlation_id()
    set_user_context("user123", "session456")
    
    # Ejemplos de logging
    logger.info("Aplicación iniciada", version="1.0.0", environment="development")
    
    @log_execution_time(logger)
    def ejemplo_funcion(x: int, y: int) -> int:
        time.sleep(0.1)  # Simular trabajo
        return x + y
    
    resultado = ejemplo_funcion(5, 3)
    logger.info("Resultado calculado", resultado=resultado)
    
    # Ejemplo de métricas
    metrics_logger.log_data_quality_metric(
        table_name="payments",
        metric_name="null_percentage",
        metric_value=2.5,
        threshold=5.0,
        passed=True
    )