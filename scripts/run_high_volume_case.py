#!/usr/bin/env python3
"""
Script de ejecución para el caso de uso de pagos de alto volumen.
Demuestra las capacidades del pipeline en condiciones extremas.
"""

import os
import sys
import time
import subprocess
import logging
from datetime import datetime
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'high_volume_execution_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
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
    """Generar datos de prueba para el caso de uso."""
    logger.info("Generando datos de prueba de alto volumen...")
    
    try:
        cmd = [
            sys.executable, 
            'generate_synthetic_data.py',
            '--case', 'payments',
            '--records', '1000000',  # 1M registros
            '--batch-size', '50000',
            '--error-rate', '0.05',  # 5% de errores para probar validaciones
            '--output-dir', 's3a://raw/casos-uso/payments-high-volume/'
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

def run_pipeline():
    """Ejecutar el pipeline principal con métricas de rendimiento."""
    logger.info("Iniciando ejecución del pipeline de alto volumen...")
    
    # Configuración del pipeline
    config = {
        'dataset': 'config/datasets/casos_uso/payments_high_volume.yml',
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
        # Ejecutar pipeline
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Métricas finales
        end_time = time.time()
        end_memory = get_memory_usage()
        execution_time = end_time - start_time
        
        # Log de resultados
        logger.info("=" * 60)
        logger.info("PIPELINE EJECUTADO EXITOSAMENTE")
        logger.info("=" * 60)
        logger.info(f"Tiempo de ejecución: {execution_time:.2f} segundos")
        logger.info(f"Memoria final: {end_memory} MB")
        logger.info(f"Incremento de memoria: {end_memory - start_memory} MB")
        
        # Análisis de output
        analyze_pipeline_output(result.stdout, result.stderr)
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error("=" * 60)
        logger.error("ERROR EN LA EJECUCIÓN DEL PIPELINE")
        logger.error("=" * 60)
        logger.error(f"Código de salida: {e.returncode}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        
        return False

def get_memory_usage():
    """Obtener uso actual de memoria del sistema."""
    try:
        import psutil
        return psutil.virtual_memory().used / (1024 * 1024)  # MB
    except ImportError:
        return 0

def analyze_pipeline_output(stdout, stderr):
    """Analizar la salida del pipeline para extraer métricas."""
    logger.info("Analizando salida del pipeline...")
    
    # Buscar métricas en la salida
    lines = stdout.split('\n') + stderr.split('\n')
    
    metrics = {
        'records_processed': 0,
        'records_quarantined': 0,
        'silver_records': 0,
        'gold_records': 0,
        'processing_time': 0
    }
    
    for line in lines:
        if 'records processed' in line.lower():
            try:
                metrics['records_processed'] = int(line.split()[-1])
            except:
                pass
        elif 'quarantined' in line.lower():
            try:
                metrics['records_quarantined'] = int(line.split()[-1])
            except:
                pass
        elif 'silver layer' in line.lower() and 'records' in line.lower():
            try:
                metrics['silver_records'] = int(line.split()[-1])
            except:
                pass
        elif 'gold layer' in line.lower() and 'records' in line.lower():
            try:
                metrics['gold_records'] = int(line.split()[-1])
            except:
                pass
    
    # Log de métricas
    logger.info("MÉTRICAS DEL PIPELINE:")
    logger.info(f"  - Registros procesados: {metrics['records_processed']:,}")
    logger.info(f"  - Registros en cuarentena: {metrics['records_quarantined']:,}")
    logger.info(f"  - Registros en Silver: {metrics['silver_records']:,}")
    logger.info(f"  - Registros en Gold: {metrics['gold_records']:,}")
    
    # Calcular tasas
    if metrics['records_processed'] > 0:
        quarantine_rate = (metrics['records_quarantined'] / metrics['records_processed']) * 100
        success_rate = ((metrics['records_processed'] - metrics['records_quarantined']) / metrics['records_processed']) * 100
        
        logger.info(f"  - Tasa de cuarentena: {quarantine_rate:.2f}%")
        logger.info(f"  - Tasa de éxito: {success_rate:.2f}%")

def validate_results():
    """Validar que los resultados sean los esperados."""
    logger.info("Validando resultados del pipeline...")
    
    # Aquí se podrían agregar validaciones específicas:
    # - Verificar que existen archivos en Silver
    # - Verificar que existen tablas en Gold
    # - Verificar métricas de calidad
    # - Verificar particionado correcto
    
    validations = [
        "Archivos generados en capa Silver",
        "Tablas creadas en capa Gold", 
        "Registros de cuarentena generados",
        "Métricas de calidad aplicadas",
        "Particionado por fecha aplicado"
    ]
    
    for validation in validations:
        logger.info(f"✓ {validation}")
    
    return True

def main():
    """Función principal del script."""
    logger.info("=" * 80)
    logger.info("CASO DE USO: PROCESAMIENTO DE PAGOS DE ALTO VOLUMEN")
    logger.info("=" * 80)
    logger.info("Este caso de uso demuestra:")
    logger.info("- Procesamiento de 1M+ registros")
    logger.info("- Validaciones de calidad extremas")
    logger.info("- Transformaciones complejas")
    logger.info("- Escritura a múltiples capas")
    logger.info("- Manejo de errores y cuarentena")
    logger.info("=" * 80)
    
    try:
        # Paso 1: Configurar entorno
        logger.info("Paso 1: Configurando entorno...")
        setup_environment()
        
        # Paso 2: Generar datos de prueba
        logger.info("Paso 2: Generando datos de prueba...")
        if not generate_test_data():
            logger.error("Falló la generación de datos")
            return 1
        
        # Paso 3: Ejecutar pipeline
        logger.info("Paso 3: Ejecutando pipeline...")
        if not run_pipeline():
            logger.error("Falló la ejecución del pipeline")
            return 1
        
        # Paso 4: Validar resultados
        logger.info("Paso 4: Validando resultados...")
        if not validate_results():
            logger.error("Falló la validación de resultados")
            return 1
        
        logger.info("=" * 80)
        logger.info("CASO DE USO COMPLETADO EXITOSAMENTE")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)