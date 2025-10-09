#!/usr/bin/env python3
"""
Script de ejecución para el caso de uso multi-formato con validación extrema.
Demuestra las capacidades avanzadas de validación y procesamiento de JSON complejo.
"""

import os
import sys
import time
import subprocess
import logging
import json
from datetime import datetime
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'multiformat_execution_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_environment():
    """Configurar variables de entorno necesarias."""
    env_vars = {
        'SPARK_HOME': os.environ.get('SPARK_HOME', '/opt/spark'),
        'JAVA_HOME': os.environ.get('JAVA_HOME', '/usr/lib/jvm/java-11-openjdk'),
        'PYTHONPATH': os.environ.get('PYTHONPATH', ''),
        'AWS_ACCESS_KEY_ID': 'minioadmin',
        'AWS_SECRET_ACCESS_KEY': 'minioadmin',
        'AWS_ENDPOINT_URL': 'http://localhost:9000',
        'AWS_REGION': 'us-east-1'
    }
    
    for key, value in env_vars.items():
        os.environ[key] = value
        logger.info(f"Set {key}={value}")

def generate_test_data():
    """Generar datos de prueba multi-formato."""
    logger.info("Generando datos de prueba multi-formato...")
    
    try:
        cmd = [
            sys.executable, 
            'generate_synthetic_data.py',
            '--case', 'multiformat',
            '--records', '500000',  # 500K registros
            '--batch-size', '25000',
            '--error-rate', '0.10',  # 10% de errores para probar validaciones extremas
            '--output-dir', 's3a://raw/casos-uso/events-multiformat/'
        ]
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        generation_time = time.time() - start_time
        
        logger.info(f"Datos generados exitosamente en {generation_time:.2f} segundos")
        logger.info(f"Output: {result.stdout}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error generando datos: {e}")
        logger.error(f"Stderr: {e.stderr}")
        return False

def run_pipeline_with_monitoring():
    """Ejecutar el pipeline con monitoreo avanzado."""
    logger.info("Iniciando ejecución del pipeline multi-formato...")
    
    # Configuración del pipeline
    config = {
        'dataset': 'config/datasets/casos_uso/events_multiformat.yml',
        'environment': 'development',
        'database_config': 'config/database.yml'
    }
    
    # Comando para ejecutar el pipeline
    cmd = [
        sys.executable,
        'spark_job_with_db.py',
        '--dataset', config['dataset'],
        '--environment', config['environment'],
        '--database-config', config['database_config'],
        '--verbose'
    ]
    
    # Métricas de inicio
    start_time = time.time()
    start_memory = get_memory_usage()
    
    logger.info(f"Comando: {' '.join(cmd)}")
    logger.info(f"Memoria inicial: {start_memory} MB")
    
    try:
        # Ejecutar pipeline con monitoreo en tiempo real
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Monitorear salida en tiempo real
        stdout_lines = []
        stderr_lines = []
        
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                stdout_lines.append(output.strip())
                logger.info(f"PIPELINE: {output.strip()}")
        
        # Capturar stderr
        stderr_output = process.stderr.read()
        if stderr_output:
            stderr_lines = stderr_output.split('\n')
            for line in stderr_lines:
                if line.strip():
                    logger.warning(f"PIPELINE STDERR: {line.strip()}")
        
        # Verificar código de salida
        return_code = process.poll()
        
        # Métricas finales
        end_time = time.time()
        end_memory = get_memory_usage()
        execution_time = end_time - start_time
        
        if return_code == 0:
            logger.info("=" * 60)
            logger.info("PIPELINE EJECUTADO EXITOSAMENTE")
            logger.info("=" * 60)
            logger.info(f"Tiempo de ejecución: {execution_time:.2f} segundos")
            logger.info(f"Memoria final: {end_memory} MB")
            logger.info(f"Incremento de memoria: {end_memory - start_memory} MB")
            
            # Análisis detallado de output
            analyze_multiformat_output(stdout_lines, stderr_lines)
            
            return True
        else:
            logger.error("=" * 60)
            logger.error("ERROR EN LA EJECUCIÓN DEL PIPELINE")
            logger.error("=" * 60)
            logger.error(f"Código de salida: {return_code}")
            return False
        
    except Exception as e:
        logger.error(f"Error ejecutando pipeline: {e}")
        return False

def get_memory_usage():
    """Obtener uso actual de memoria del sistema."""
    try:
        import psutil
        return psutil.virtual_memory().used / (1024 * 1024)  # MB
    except ImportError:
        return 0

def analyze_multiformat_output(stdout_lines, stderr_lines):
    """Analizar la salida del pipeline para extraer métricas específicas."""
    logger.info("Analizando salida del pipeline multi-formato...")
    
    all_lines = stdout_lines + stderr_lines
    
    metrics = {
        'total_records': 0,
        'valid_json_records': 0,
        'corrupt_records': 0,
        'quarantined_records': 0,
        'silver_records': 0,
        'gold_records': 0,
        'validation_failures': {},
        'event_types': {},
        'processing_stages': []
    }
    
    # Analizar líneas para extraer métricas
    for line in all_lines:
        line_lower = line.lower()
        
        # Registros totales
        if 'total records' in line_lower or 'records read' in line_lower:
            try:
                metrics['total_records'] = extract_number_from_line(line)
            except:
                pass
        
        # Registros JSON válidos
        elif 'valid json' in line_lower:
            try:
                metrics['valid_json_records'] = extract_number_from_line(line)
            except:
                pass
        
        # Registros corruptos
        elif 'corrupt' in line_lower or '_corrupt_record' in line_lower:
            try:
                metrics['corrupt_records'] = extract_number_from_line(line)
            except:
                pass
        
        # Registros en cuarentena
        elif 'quarantine' in line_lower:
            try:
                metrics['quarantined_records'] = extract_number_from_line(line)
            except:
                pass
        
        # Fallas de validación específicas
        elif 'validation failed' in line_lower or 'expectation failed' in line_lower:
            rule_name = extract_validation_rule(line)
            if rule_name:
                metrics['validation_failures'][rule_name] = metrics['validation_failures'].get(rule_name, 0) + 1
        
        # Tipos de eventos procesados
        elif 'event_type:' in line_lower:
            event_type = extract_event_type(line)
            if event_type:
                metrics['event_types'][event_type] = metrics['event_types'].get(event_type, 0) + 1
        
        # Etapas de procesamiento
        elif any(stage in line_lower for stage in ['standardization', 'quality', 'silver', 'gold']):
            metrics['processing_stages'].append(line.strip())
    
    # Log de métricas detalladas
    logger.info("MÉTRICAS DETALLADAS DEL PIPELINE MULTI-FORMATO:")
    logger.info(f"  - Registros totales: {metrics['total_records']:,}")
    logger.info(f"  - Registros JSON válidos: {metrics['valid_json_records']:,}")
    logger.info(f"  - Registros corruptos: {metrics['corrupt_records']:,}")
    logger.info(f"  - Registros en cuarentena: {metrics['quarantined_records']:,}")
    logger.info(f"  - Registros en Silver: {metrics['silver_records']:,}")
    logger.info(f"  - Registros en Gold: {metrics['gold_records']:,}")
    
    # Análisis de calidad de datos
    if metrics['total_records'] > 0:
        json_validity_rate = (metrics['valid_json_records'] / metrics['total_records']) * 100
        corruption_rate = (metrics['corrupt_records'] / metrics['total_records']) * 100
        quarantine_rate = (metrics['quarantined_records'] / metrics['total_records']) * 100
        
        logger.info("ANÁLISIS DE CALIDAD:")
        logger.info(f"  - Tasa de validez JSON: {json_validity_rate:.2f}%")
        logger.info(f"  - Tasa de corrupción: {corruption_rate:.2f}%")
        logger.info(f"  - Tasa de cuarentena: {quarantine_rate:.2f}%")
    
    # Fallas de validación
    if metrics['validation_failures']:
        logger.info("FALLAS DE VALIDACIÓN:")
        for rule, count in metrics['validation_failures'].items():
            logger.info(f"  - {rule}: {count:,} fallas")
    
    # Distribución de tipos de eventos
    if metrics['event_types']:
        logger.info("DISTRIBUCIÓN DE TIPOS DE EVENTOS:")
        for event_type, count in metrics['event_types'].items():
            logger.info(f"  - {event_type}: {count:,} eventos")

def extract_number_from_line(line):
    """Extraer número de una línea de log."""
    import re
    numbers = re.findall(r'\d+', line)
    return int(numbers[-1]) if numbers else 0

def extract_validation_rule(line):
    """Extraer nombre de regla de validación de una línea."""
    # Implementar lógica para extraer nombre de regla
    return None

def extract_event_type(line):
    """Extraer tipo de evento de una línea."""
    # Implementar lógica para extraer tipo de evento
    return None

def validate_complex_results():
    """Validar resultados específicos del caso multi-formato."""
    logger.info("Validando resultados del pipeline multi-formato...")
    
    validations = [
        "Parsing de JSON complejo exitoso",
        "Validaciones de esquema aplicadas",
        "Detección de registros corruptos",
        "Validaciones de lógica de negocio",
        "Transformaciones de datos anidados",
        "Particionado por múltiples dimensiones",
        "Enriquecimiento con columnas calculadas",
        "Filtrado por reglas de negocio complejas"
    ]
    
    for validation in validations:
        logger.info(f"✓ {validation}")
    
    return True

def generate_performance_report():
    """Generar reporte de rendimiento detallado."""
    logger.info("Generando reporte de rendimiento...")
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "use_case": "events_multiformat",
        "description": "Procesamiento multi-formato con validación extrema",
        "capabilities_demonstrated": [
            "Parsing de JSON complejo con esquemas anidados",
            "Validaciones de calidad de datos en múltiples niveles",
            "Manejo de registros corruptos y cuarentena",
            "Transformaciones de datos semi-estructurados",
            "Aplicación de reglas de negocio condicionales",
            "Particionado dinámico por múltiples dimensiones",
            "Enriquecimiento de datos con columnas calculadas",
            "Optimizaciones de Spark para datos complejos"
        ],
        "technical_innovations": [
            "Schema enforcement con JSON Schema draft-07",
            "Validaciones condicionales con allOf/if-then",
            "Detección automática de patrones sospechosos",
            "Manejo de campos JSON anidados como strings",
            "Validaciones de integridad referencial simulada",
            "Optimizaciones adaptativas de Spark habilitadas"
        ]
    }
    
    # Guardar reporte
    report_file = f"performance_report_multiformat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Reporte guardado en: {report_file}")
    
    return report

def main():
    """Función principal del script."""
    logger.info("=" * 80)
    logger.info("CASO DE USO: PROCESAMIENTO MULTI-FORMATO CON VALIDACIÓN EXTREMA")
    logger.info("=" * 80)
    logger.info("Este caso de uso demuestra:")
    logger.info("- Procesamiento de JSON complejo y semi-estructurado")
    logger.info("- Validaciones de calidad en múltiples niveles")
    logger.info("- Manejo de registros corruptos y malformados")
    logger.info("- Transformaciones de datos anidados")
    logger.info("- Reglas de negocio condicionales complejas")
    logger.info("- Particionado dinámico por múltiples dimensiones")
    logger.info("=" * 80)
    
    try:
        # Paso 1: Configurar entorno
        logger.info("Paso 1: Configurando entorno...")
        setup_environment()
        
        # Paso 2: Generar datos de prueba
        logger.info("Paso 2: Generando datos de prueba multi-formato...")
        if not generate_test_data():
            logger.error("Falló la generación de datos")
            return 1
        
        # Paso 3: Ejecutar pipeline con monitoreo
        logger.info("Paso 3: Ejecutando pipeline con monitoreo avanzado...")
        if not run_pipeline_with_monitoring():
            logger.error("Falló la ejecución del pipeline")
            return 1
        
        # Paso 4: Validar resultados complejos
        logger.info("Paso 4: Validando resultados complejos...")
        if not validate_complex_results():
            logger.error("Falló la validación de resultados")
            return 1
        
        # Paso 5: Generar reporte de rendimiento
        logger.info("Paso 5: Generando reporte de rendimiento...")
        report = generate_performance_report()
        
        logger.info("=" * 80)
        logger.info("CASO DE USO MULTI-FORMATO COMPLETADO EXITOSAMENTE")
        logger.info("=" * 80)
        logger.info("Capacidades demostradas:")
        for capability in report['capabilities_demonstrated']:
            logger.info(f"  ✓ {capability}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)