#!/usr/bin/env python3
"""
Script de pruebas para validar el funcionamiento del pipeline en contenedor Docker.
"""

import os
import sys
import json
import time
import subprocess
import tempfile
import shutil
from pathlib import Path
import pandas as pd
import pytest

# Configurar paths
PROJECT_ROOT = Path(__file__).parent.parent
DOCKER_DIR = PROJECT_ROOT / "docker"
TEST_DATA_DIR = PROJECT_ROOT / "test_data"

class DockerPipelineTest:
    """Clase para pruebas del pipeline en Docker."""
    
    def __init__(self):
        self.image_name = "mvp-pipeline:test"
        self.container_name = "mvp-pipeline-test"
        self.test_output_dir = PROJECT_ROOT / "test_output"
        
    def setup(self):
        """Configuración inicial para las pruebas."""
        # Crear directorio de salida de pruebas
        self.test_output_dir.mkdir(exist_ok=True)
        
        # Limpiar contenedores previos
        self.cleanup()
        
    def cleanup(self):
        """Limpiar recursos de prueba."""
        try:
            # Detener y eliminar contenedor si existe
            subprocess.run([
                "docker", "rm", "-f", self.container_name
            ], capture_output=True, check=False)
        except Exception:
            pass
    
    def build_image(self):
        """Construir la imagen Docker para pruebas."""
        print("🔨 Construyendo imagen Docker...")
        
        build_cmd = [
            "docker", "build",
            "-f", str(DOCKER_DIR / "Dockerfile.pipeline"),
            "-t", self.image_name,
            str(PROJECT_ROOT)
        ]
        
        result = subprocess.run(build_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Error construyendo imagen: {result.stderr}")
            return False
        
        print("✅ Imagen construida exitosamente")
        return True
    
    def test_basic_execution(self):
        """Prueba básica de ejecución del contenedor."""
        print("🧪 Ejecutando prueba básica...")
        
        # Preparar archivos de prueba
        input_file = "test_data/sample_data.csv"
        output_file = "test_output/basic_test_output.parquet"
        
        # Comando Docker
        docker_cmd = [
            "docker", "run", "--rm",
            "--name", self.container_name,
            "-v", f"{PROJECT_ROOT}:/app/host_data",
            self.image_name,
            f"/app/host_data/{input_file}",
            f"/app/host_data/{output_file}",
            "--test-mode",
            "--log-level", "DEBUG"
        ]
        
        print(f"Ejecutando: {' '.join(docker_cmd)}")
        
        result = subprocess.run(docker_cmd, capture_output=True, text=True, timeout=300)
        
        print(f"Código de salida: {result.returncode}")
        print(f"Stdout: {result.stdout}")
        if result.stderr:
            print(f"Stderr: {result.stderr}")
        
        # Verificar resultado
        if result.returncode == 0:
            try:
                output_data = json.loads(result.stdout.strip().split('\n')[-1])
                if output_data.get('status') == 'success':
                    print("✅ Prueba básica exitosa")
                    return True
                else:
                    print(f"❌ Pipeline falló: {output_data.get('error')}")
                    return False
            except json.JSONDecodeError:
                print("❌ Error parseando salida JSON")
                return False
        else:
            print(f"❌ Contenedor falló con código: {result.returncode}")
            return False
    
    def test_file_processing(self):
        """Prueba de procesamiento de archivos."""
        print("🧪 Ejecutando prueba de procesamiento de archivos...")
        
        # Crear archivo de prueba temporal
        test_data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
            'age': [25, 30, 35, 28, 32],
            'city': ['Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Bilbao']
        }
        
        test_df = pd.DataFrame(test_data)
        temp_input = self.test_output_dir / "temp_input.csv"
        temp_output = self.test_output_dir / "temp_output.parquet"
        
        # Guardar archivo de prueba
        test_df.to_csv(temp_input, index=False)
        
        # Ejecutar contenedor
        docker_cmd = [
            "docker", "run", "--rm",
            "--name", self.container_name,
            "-v", f"{PROJECT_ROOT}:/app/host_data",
            self.image_name,
            f"/app/host_data/{temp_input.relative_to(PROJECT_ROOT)}",
            f"/app/host_data/{temp_output.relative_to(PROJECT_ROOT)}",
            "--log-level", "INFO"
        ]
        
        result = subprocess.run(docker_cmd, capture_output=True, text=True, timeout=300)
        
        # Verificar resultado
        if result.returncode == 0 and temp_output.exists():
            # Verificar contenido del archivo de salida
            try:
                output_df = pd.read_parquet(temp_output)
                if len(output_df) == len(test_df):
                    print("✅ Prueba de procesamiento exitosa")
                    return True
                else:
                    print(f"❌ Número de filas incorrecto: {len(output_df)} vs {len(test_df)}")
                    return False
            except Exception as e:
                print(f"❌ Error leyendo archivo de salida: {e}")
                return False
        else:
            print(f"❌ Prueba falló - Código: {result.returncode}")
            if result.stderr:
                print(f"Error: {result.stderr}")
            return False
    
    def test_error_handling(self):
        """Prueba de manejo de errores."""
        print("🧪 Ejecutando prueba de manejo de errores...")
        
        # Intentar procesar archivo inexistente
        docker_cmd = [
            "docker", "run", "--rm",
            "--name", self.container_name,
            "-v", f"{PROJECT_ROOT}:/app/host_data",
            self.image_name,
            "/app/host_data/nonexistent_file.csv",
            "/app/host_data/test_output/error_test.parquet",
            "--log-level", "ERROR"
        ]
        
        result = subprocess.run(docker_cmd, capture_output=True, text=True, timeout=60)
        
        # Debe fallar con código de error
        if result.returncode != 0:
            try:
                # Verificar que la salida contiene información de error
                if result.stdout and ('error' in result.stdout.lower() or 'failed' in result.stdout.lower()):
                    print("✅ Prueba de manejo de errores exitosa")
                    return True
                else:
                    print("❌ Error no reportado correctamente")
                    return False
            except Exception:
                print("✅ Prueba de manejo de errores exitosa (falló como esperado)")
                return True
        else:
            print("❌ Debería haber fallado con archivo inexistente")
            return False
    
    def run_all_tests(self):
        """Ejecutar todas las pruebas."""
        print("🚀 Iniciando pruebas del pipeline Docker...")
        
        self.setup()
        
        # Construir imagen
        if not self.build_image():
            print("❌ No se pudo construir la imagen")
            return False
        
        # Ejecutar pruebas
        tests = [
            ("Prueba básica", self.test_basic_execution),
            ("Procesamiento de archivos", self.test_file_processing),
            ("Manejo de errores", self.test_error_handling)
        ]
        
        results = []
        for test_name, test_func in tests:
            print(f"\n--- {test_name} ---")
            try:
                success = test_func()
                results.append((test_name, success))
            except Exception as e:
                print(f"❌ Error en {test_name}: {e}")
                results.append((test_name, False))
            finally:
                self.cleanup()
        
        # Resumen de resultados
        print("\n" + "="*50)
        print("📊 RESUMEN DE PRUEBAS")
        print("="*50)
        
        passed = 0
        for test_name, success in results:
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"{status} - {test_name}")
            if success:
                passed += 1
        
        print(f"\nResultado: {passed}/{len(results)} pruebas exitosas")
        
        if passed == len(results):
            print("🎉 ¡Todas las pruebas pasaron!")
            return True
        else:
            print("⚠️  Algunas pruebas fallaron")
            return False

def main():
    """Función principal."""
    tester = DockerPipelineTest()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()