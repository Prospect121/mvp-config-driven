#!/usr/bin/env python3
"""
Script de entrada mejorado para el contenedor del pipeline.
Maneja argumentos, configuración y logging de manera robusta.
"""

import sys
import os
import json
import logging
import argparse
from pathlib import Path

# Configurar paths para importaciones
sys.path.insert(0, '/app')
sys.path.insert(0, '/app/src')

def setup_logging(log_level: str = "INFO"):
    """Configurar logging para el contenedor."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('/app/logs/pipeline.log', mode='a')
        ]
    )
    return logging.getLogger(__name__)

def validate_paths(input_path: str, output_path: str) -> tuple:
    """Validar y normalizar rutas de entrada y salida."""
    # Convertir rutas relativas a absolutas
    if not os.path.isabs(input_path):
        input_path = os.path.join('/app', input_path)
    
    if not os.path.isabs(output_path):
        output_path = os.path.join('/app', output_path)
    
    # Crear directorio de salida si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    return input_path, output_path

def main():
    """Función principal del script de entrada."""
    parser = argparse.ArgumentParser(description='Pipeline de procesamiento de datos containerizado')
    parser.add_argument('input_path', help='Ruta del archivo de entrada')
    parser.add_argument('output_path', help='Ruta del archivo de salida')
    parser.add_argument('--config', default='/app/config/data_factory_config.yml', 
                       help='Ruta del archivo de configuración')
    parser.add_argument('--expectations', default='/app/config/expectations.yml',
                       help='Ruta del archivo de expectativas')
    parser.add_argument('--env-config', default='/app/config/env_local.yml',
                       help='Ruta del archivo de configuración de entorno')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Nivel de logging')
    parser.add_argument('--test-mode', action='store_true',
                       help='Ejecutar en modo de prueba con datos de ejemplo')
    
    args = parser.parse_args()
    
    # Configurar logging
    logger = setup_logging(args.log_level)
    logger.info("Iniciando pipeline containerizado")
    logger.info(f"Argumentos: {vars(args)}")
    
    try:
        # Validar rutas
        input_path, output_path = validate_paths(args.input_path, args.output_path)
        logger.info(f"Rutas validadas - Input: {input_path}, Output: {output_path}")
        
        # Modo de prueba con datos de ejemplo
        if args.test_mode:
            test_data_path = '/app/test_data/sample_data.csv'
            if os.path.exists(test_data_path):
                input_path = test_data_path
                logger.info(f"Modo de prueba activado, usando: {input_path}")
            else:
                logger.warning("Archivo de prueba no encontrado, usando input_path original")
        
        # Verificar que el archivo de entrada existe
        if not os.path.exists(input_path):
            logger.error(f"Archivo de entrada no encontrado: {input_path}")
            sys.exit(1)
        
        # Establecer modo de prueba si está habilitado
        if args.test_mode:
            os.environ['TEST_MODE'] = 'true'
            logger.info("Modo de prueba activado - omitiendo configuraciones externas")
        
        # Importar y ejecutar pipeline
        from src.integrated_pipeline import IntegratedDataPipeline
        
        # Crear pipeline integrado
        pipeline = IntegratedDataPipeline(
            config_path=args.config,
            expectations_path=args.expectations,
            env_path=args.env_config
        )
        
        # Procesar datos
        logger.info("Iniciando procesamiento de datos...")
        result = pipeline.process_data(input_path, output_path)
        
        # Imprimir resultado como JSON
        print(json.dumps(result, indent=2))
        
        # Logging del resultado
        if result['status'] == 'success':
            logger.info("Pipeline completado exitosamente")
            logger.info(f"Métricas: {result.get('metrics', {})}")
        else:
            logger.error(f"Pipeline falló: {result.get('error', 'Error desconocido')}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error crítico en el pipeline: {str(e)}", exc_info=True)
        error_result = {
            'status': 'failed',
            'error': str(e),
            'error_type': type(e).__name__
        }
        print(json.dumps(error_result, indent=2))
        sys.exit(1)

if __name__ == "__main__":
    main()