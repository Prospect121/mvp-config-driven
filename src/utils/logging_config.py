"""
Configuración de logging estructurado para observabilidad.
"""
import os
import sys
import logging
import json
from datetime import datetime
from typing import Dict, Any

class StructuredFormatter(logging.Formatter):
    """Formateador de logs estructurados en JSON"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Añadir información adicional si está disponible
        if hasattr(record, 'pipeline_id'):
            log_entry['pipeline_id'] = record.pipeline_id
        
        if hasattr(record, 'run_id'):
            log_entry['run_id'] = record.run_id
        
        if hasattr(record, 'dataset_id'):
            log_entry['dataset_id'] = record.dataset_id
        
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, ensure_ascii=False)

def setup_logging(level: str = None) -> None:
    """
    Configura el sistema de logging.
    
    Args:
        level: Nivel de logging (DEBUG, INFO, WARNING, ERROR)
    """
    log_level = level or os.getenv('LOG_LEVEL', 'INFO').upper()
    
    # Configurar el logger raíz
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))
    
    # Limpiar handlers existentes
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Crear handler para stdout
    handler = logging.StreamHandler(sys.stdout)
    
    # Usar formato estructurado en producción, simple en desarrollo
    if os.getenv('ENVIRONMENT', 'local') == 'local':
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    else:
        formatter = StructuredFormatter()
    
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    
    # Configurar loggers específicos
    logging.getLogger('azure').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('pyspark').setLevel(logging.WARNING)

class PipelineLogger:
    """Logger contextual para pipelines"""
    
    def __init__(self, pipeline_id: str, run_id: str = None):
        self.pipeline_id = pipeline_id
        self.run_id = run_id or datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
        self.logger = logging.getLogger(f"pipeline.{pipeline_id}")
    
    def _add_context(self, extra: Dict[str, Any] = None) -> Dict[str, Any]:
        """Añade contexto del pipeline a los logs"""
        context = {
            'pipeline_id': self.pipeline_id,
            'run_id': self.run_id
        }
        if extra:
            context.update(extra)
        return context
    
    def info(self, message: str, **kwargs) -> None:
        self.logger.info(message, extra=self._add_context(kwargs))
    
    def warning(self, message: str, **kwargs) -> None:
        self.logger.warning(message, extra=self._add_context(kwargs))
    
    def error(self, message: str, **kwargs) -> None:
        self.logger.error(message, extra=self._add_context(kwargs))
    
    def debug(self, message: str, **kwargs) -> None:
        self.logger.debug(message, extra=self._add_context(kwargs))