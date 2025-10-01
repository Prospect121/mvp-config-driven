"""
Pruebas unitarias para el sistema de logging estructurado.
"""

import pytest
import json
import logging
import uuid
from datetime import datetime
from unittest.mock import patch, MagicMock
from io import StringIO
import sys

from src.utils.logging import (
    StructuredLogger, 
    StructuredFormatter,
    get_logger,
    set_correlation_id,
    set_user_context,
    log_execution_time,
    log_pipeline_step,
    MetricsLogger,
    setup_logging,
    correlation_id,
    user_id,
    session_id
)

class TestStructuredFormatter:
    """Pruebas para el formateador estructurado."""
    
    def test_format_basic_log(self):
        """Prueba formateo básico de log."""
        formatter = StructuredFormatter("test-service")
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/test/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        assert log_data["level"] == "INFO"
        assert log_data["logger"] == "test.logger"
        assert log_data["message"] == "Test message"
        assert log_data["service"] == "test-service"
        assert "timestamp" in log_data
        assert "location" in log_data
        assert log_data["location"]["file"] == "/test/file.py"
        assert log_data["location"]["line"] == 42
    
    def test_format_with_context(self):
        """Prueba formateo con contexto de trazabilidad."""
        correlation_id.set("test-correlation-123")
        user_id.set("user-456")
        session_id.set("session-789")
        
        formatter = StructuredFormatter("test-service")
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/test/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        assert log_data["context"]["correlation_id"] == "test-correlation-123"
        assert log_data["context"]["user_id"] == "user-456"
        assert log_data["context"]["session_id"] == "session-789"
    
    def test_format_with_extra_data(self):
        """Prueba formateo con datos adicionales."""
        formatter = StructuredFormatter("test-service")
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/test/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None
        )
        record.extra_data = {"key1": "value1", "key2": 123}
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        assert log_data["data"]["key1"] == "value1"
        assert log_data["data"]["key2"] == 123
    
    def test_format_with_exception(self):
        """Prueba formateo con información de excepción."""
        formatter = StructuredFormatter("test-service")
        
        try:
            raise ValueError("Test exception")
        except ValueError:
            record = logging.LogRecord(
                name="test.logger",
                level=logging.ERROR,
                pathname="/test/file.py",
                lineno=42,
                msg="Error occurred",
                args=(),
                exc_info=sys.exc_info()
            )
        
        formatted = formatter.format(record)
        log_data = json.loads(formatted)
        
        assert "exception" in log_data
        assert log_data["exception"]["type"] == "ValueError"
        assert log_data["exception"]["message"] == "Test exception"
        assert "traceback" in log_data["exception"]

class TestStructuredLogger:
    """Pruebas para el logger estructurado."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.output = StringIO()
        self.handler = logging.StreamHandler(self.output)
        self.logger = StructuredLogger("test.logger")
        
        # Limpiar handlers existentes y agregar nuestro handler de prueba
        self.logger.logger.handlers.clear()
        self.logger.logger.addHandler(self.handler)
        self.logger.logger.setLevel(logging.DEBUG)
    
    def teardown_method(self):
        """Limpieza después de cada prueba."""
        self.logger.logger.handlers.clear()
    
    def test_info_logging(self):
        """Prueba logging de información."""
        self.logger.info("Test info message", key1="value1", key2=123)
        
        output = self.output.getvalue()
        log_data = json.loads(output.strip())
        
        assert log_data["level"] == "INFO"
        assert log_data["message"] == "Test info message"
        assert log_data["data"]["key1"] == "value1"
        assert log_data["data"]["key2"] == 123
    
    def test_error_logging_with_exception(self):
        """Prueba logging de error con excepción."""
        test_exception = ValueError("Test error")
        self.logger.error("Error occurred", exception=test_exception, context="test")
        
        output = self.output.getvalue()
        log_data = json.loads(output.strip())
        
        assert log_data["level"] == "ERROR"
        assert log_data["message"] == "Error occurred"
        assert log_data["data"]["context"] == "test"
        assert "exception" in log_data
    
    def test_debug_logging(self):
        """Prueba logging de debug."""
        self.logger.debug("Debug message", debug_info="test")
        
        output = self.output.getvalue()
        log_data = json.loads(output.strip())
        
        assert log_data["level"] == "DEBUG"
        assert log_data["message"] == "Debug message"
        assert log_data["data"]["debug_info"] == "test"

class TestLoggingUtilities:
    """Pruebas para utilidades de logging."""
    
    def test_set_correlation_id(self):
        """Prueba establecimiento de correlation ID."""
        # Test con ID específico
        test_id = "test-correlation-123"
        result = set_correlation_id(test_id)
        assert result == test_id
        assert correlation_id.get() == test_id
        
        # Test con ID generado automáticamente
        auto_id = set_correlation_id()
        assert auto_id is not None
        assert len(auto_id) > 0
        assert correlation_id.get() == auto_id
    
    def test_set_user_context(self):
        """Prueba establecimiento de contexto de usuario."""
        set_user_context("user123", "session456")
        
        assert user_id.get() == "user123"
        assert session_id.get() == "session456"
        
        # Test sin session ID
        set_user_context("user789")
        assert user_id.get() == "user789"
    
    def test_get_logger(self):
        """Prueba factory function para logger."""
        logger = get_logger("test.module")
        
        assert isinstance(logger, StructuredLogger)
        assert logger.logger.name == "test.module"

class TestLoggingDecorators:
    """Pruebas para decoradores de logging."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.output = StringIO()
        self.handler = logging.StreamHandler(self.output)
        
        # Configurar logging para capturar salida
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.addHandler(self.handler)
        root_logger.setLevel(logging.DEBUG)
    
    def teardown_method(self):
        """Limpieza después de cada prueba."""
        logging.getLogger().handlers.clear()
    
    def test_log_execution_time_decorator(self):
        """Prueba decorador de tiempo de ejecución."""
        logger = get_logger("test.decorator")
        
        @log_execution_time(logger)
        def test_function(x, y):
            return x + y
        
        result = test_function(5, 3)
        assert result == 8
        
        output = self.output.getvalue()
        log_lines = [line for line in output.strip().split('\n') if line]
        
        # Debe haber al menos 2 logs: inicio y fin
        assert len(log_lines) >= 2
        
        # Verificar log de inicio
        start_log = json.loads(log_lines[0])
        assert "Iniciando ejecución" in start_log["message"]
        assert start_log["data"]["function"] == "test_function"
        
        # Verificar log de fin
        end_log = json.loads(log_lines[-1])
        assert "Ejecución completada" in end_log["message"]
        assert end_log["data"]["success"] is True
        assert "duration_ms" in end_log["data"]
    
    def test_log_execution_time_with_exception(self):
        """Prueba decorador con excepción."""
        logger = get_logger("test.decorator")
        
        @log_execution_time(logger)
        def failing_function():
            raise ValueError("Test error")
        
        with pytest.raises(ValueError):
            failing_function()
        
        output = self.output.getvalue()
        log_lines = [line for line in output.strip().split('\n') if line]
        
        # Verificar log de error
        error_log = json.loads(log_lines[-1])
        assert "Error en ejecución" in error_log["message"]
        assert error_log["data"]["success"] is False
    
    def test_log_pipeline_step_decorator(self):
        """Prueba decorador de paso de pipeline."""
        @log_pipeline_step("test_step", "pipeline_123")
        def pipeline_step():
            return "step_result"
        
        result = pipeline_step()
        assert result == "step_result"
        
        output = self.output.getvalue()
        log_lines = [line for line in output.strip().split('\n') if line]
        
        # Verificar logs de pipeline
        start_log = json.loads(log_lines[0])
        assert "Iniciando paso de pipeline" in start_log["message"]
        assert start_log["data"]["step_name"] == "test_step"
        assert start_log["data"]["pipeline_id"] == "pipeline_123"
        
        end_log = json.loads(log_lines[-1])
        assert "Paso de pipeline completado" in end_log["message"]
        assert end_log["data"]["success"] is True

class TestMetricsLogger:
    """Pruebas para el logger de métricas."""
    
    def setup_method(self):
        """Configuración para cada prueba."""
        self.output = StringIO()
        self.handler = logging.StreamHandler(self.output)
        
        # Configurar logging para capturar salida
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.addHandler(self.handler)
        root_logger.setLevel(logging.DEBUG)
        
        self.metrics_logger = MetricsLogger("test-service")
    
    def teardown_method(self):
        """Limpieza después de cada prueba."""
        logging.getLogger().handlers.clear()
    
    def test_log_data_quality_metric(self):
        """Prueba logging de métrica de calidad de datos."""
        self.metrics_logger.log_data_quality_metric(
            table_name="test_table",
            metric_name="null_percentage",
            metric_value=2.5,
            threshold=5.0,
            passed=True
        )
        
        output = self.output.getvalue()
        log_data = json.loads(output.strip())
        
        assert log_data["data"]["metric_type"] == "data_quality"
        assert log_data["data"]["table_name"] == "test_table"
        assert log_data["data"]["metric_name"] == "null_percentage"
        assert log_data["data"]["metric_value"] == 2.5
        assert log_data["data"]["threshold"] == 5.0
        assert log_data["data"]["passed"] is True
    
    def test_log_pipeline_metric(self):
        """Prueba logging de métrica de pipeline."""
        self.metrics_logger.log_pipeline_metric(
            pipeline_name="test_pipeline",
            metric_name="records_processed",
            metric_value=1000,
            unit="records"
        )
        
        output = self.output.getvalue()
        log_data = json.loads(output.strip())
        
        assert log_data["data"]["metric_type"] == "pipeline"
        assert log_data["data"]["pipeline_name"] == "test_pipeline"
        assert log_data["data"]["metric_name"] == "records_processed"
        assert log_data["data"]["metric_value"] == 1000
        assert log_data["data"]["unit"] == "records"
    
    def test_log_performance_metric(self):
        """Prueba logging de métrica de performance."""
        self.metrics_logger.log_performance_metric(
            operation="data_transformation",
            duration_ms=5000.0,
            records_processed=10000,
            memory_usage_mb=256.5
        )
        
        output = self.output.getvalue()
        log_data = json.loads(output.strip())
        
        assert log_data["data"]["metric_type"] == "performance"
        assert log_data["data"]["operation"] == "data_transformation"
        assert log_data["data"]["duration_ms"] == 5000.0
        assert log_data["data"]["records_processed"] == 10000
        assert log_data["data"]["memory_usage_mb"] == 256.5
        assert log_data["data"]["throughput_records_per_second"] == 2000.0

class TestLoggingIntegration:
    """Pruebas de integración para el sistema de logging."""
    
    def test_complete_logging_workflow(self):
        """Prueba flujo completo de logging."""
        # Configurar logging
        setup_logging("DEBUG", "integration-test")
        
        # Establecer contexto
        correlation_id_val = set_correlation_id("integration-test-123")
        set_user_context("test_user", "test_session")
        
        # Crear logger y loggear eventos
        logger = get_logger("integration.test")
        
        logger.info("Starting integration test", test_type="complete_workflow")
        logger.debug("Debug information", step=1)
        logger.warning("Warning message", issue="minor")
        
        # Verificar que el contexto se mantiene
        assert correlation_id.get() == correlation_id_val
        assert user_id.get() == "test_user"
        assert session_id.get() == "test_session"
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_setup_logging_configuration(self, mock_stdout):
        """Prueba configuración de logging."""
        setup_logging("INFO", "config-test")
        
        logger = get_logger("config.test")
        logger.info("Test configuration message")
        
        # Verificar que el mensaje se formateó correctamente
        output = mock_stdout.getvalue()
        if output.strip():
            log_data = json.loads(output.strip())
            assert log_data["service"] == "config-test"
            assert log_data["level"] == "INFO"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])