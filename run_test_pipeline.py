#!/usr/bin/env python3
"""Script de prueba para ejecutar el pipeline local."""

import sys
import os

# Asegurar que estamos en el directorio correcto
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Agregar el proyecto al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datacore.cli.main import main

if __name__ == "__main__":
    # Ejecutar dry-run primero para ver el plan
    print("=" * 60)
    print("EJECUTANDO DRY-RUN (solo plan, sin Spark)")
    print("=" * 60)

    try:
        sys.argv = [
            'prodi',
            'run',
            '--layer', 'raw',
            '--config', 'examples/local_test_pipeline.yaml',
            '--platform', 'local',
            '--dry-run'
        ]
        main()
        print("\n✅ Dry-run completado exitosamente")
    except Exception as e:
        print(f"\n❌ Error en dry-run: {e}")
        import traceback
        traceback.print_exc()
