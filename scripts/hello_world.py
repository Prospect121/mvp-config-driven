#!/usr/bin/env python3
"""
Script de prueba para validar la infraestructura de Airflow.
"""
import sys
from datetime import datetime


def main():
    print("=" * 50)
    print("🚀 HOLA MUNDO desde Datacore!")
    print("=" * 50)
    print(f"📅 Fecha/Hora: {datetime.now().isoformat()}")
    print(f"🐍 Python: {sys.version}")
    print(f"📂 Ejecutado desde: {__file__}")
    print("=" * 50)
    print("✅ Infraestructura funcionando correctamente!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
