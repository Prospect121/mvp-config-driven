#!/usr/bin/env python3
"""
Generador de Datos Sintéticos para Casos de Uso Avanzados
=========================================================

Este script genera datasets sintéticos realistas que ejercitan todas las 
funcionalidades del pipeline config-driven hasta sus límites actuales.

Casos de uso soportados:
1. Pagos de Alto Volumen (1M+ registros)
2. Multi-formato con Validación Extrema (500K+ registros)

Características:
- Distribuciones realistas de datos
- Errores intencionados para testing de validaciones
- Múltiples formatos de salida (CSV, JSON, Parquet)
- Configuración de volumen y complejidad
- Métricas de generación en tiempo real

Uso:
    python scripts/generate_synthetic_data.py --case payments --records 1000000
    python scripts/generate_synthetic_data.py --case multiformat --records 500000
    python scripts/generate_synthetic_data.py --case all
"""

import os
import sys
import json
import csv
import random
import uuid
import argparse
import time
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

# Configurar encoding para stdout en Windows antes del logging
if sys.platform == "win32":
    import codecs
    # Configurar stdout con UTF-8 sin detach
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')

# Configuración de logging con encoding UTF-8 para Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_generation.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

try:
    from faker import Faker
    fake = Faker(['es_ES', 'en_US', 'pt_BR'])
except ImportError:
    logger.error("Faker library not found. Install with: pip install faker")
    sys.exit(1)

class DataGenerationMetrics:
    """Clase para tracking de métricas durante la generación"""
    
    def __init__(self):
        self.start_time = time.time()
        self.records_generated = 0
        self.files_created = 0
        self.errors_introduced = 0
        self.total_size_bytes = 0
    
    def record_generated(self, with_error: bool = False):
        self.records_generated += 1
        if with_error:
            self.errors_introduced += 1
    
    def file_created(self, file_path: str):
        self.files_created += 1
        if os.path.exists(file_path):
            self.total_size_bytes += os.path.getsize(file_path)
    
    def get_summary(self) -> Dict[str, Any]:
        elapsed_time = time.time() - self.start_time
        return {
            "elapsed_time_seconds": round(elapsed_time, 2),
            "records_generated": self.records_generated,
            "files_created": self.files_created,
            "errors_introduced": self.errors_introduced,
            "error_rate_percent": round((self.errors_introduced / max(self.records_generated, 1)) * 100, 2),
            "total_size_mb": round(self.total_size_bytes / (1024 * 1024), 2),
            "records_per_second": round(self.records_generated / max(elapsed_time, 1), 2)
        }

class AdvancedSyntheticDataGenerator:
    """Generador avanzado de datos sintéticos para casos de uso extremos"""
    
    def __init__(self, output_base_dir: str = "data/casos-uso"):
        """
        Inicializa el generador de datos sintéticos
        
        Args:
            output_base_dir: Directorio base para guardar los archivos generados
        """
        # Manejar paths S3A - convertir a path local para generación
        if output_base_dir.startswith("s3a://"):
            # Extraer la parte del path después de s3a://
            s3_path = output_base_dir.replace("s3a://", "").replace("/", os.sep)
            # Crear path local equivalente
            self.output_base_dir = Path("data") / "s3a-staging" / s3_path
            self.is_s3_target = True
            self.s3_target_path = output_base_dir
            logger.info(f"[S3A] Path S3A detectado. Generando localmente en: {self.output_base_dir}")
            logger.info(f"[S3A] Target S3A: {self.s3_target_path}")
        else:
            self.output_base_dir = Path(output_base_dir)
            self.is_s3_target = False
            self.s3_target_path = None
            
        self.metrics = DataGenerationMetrics()
        
        # Configuraciones de datos realistas
        self.currencies = ["USD", "EUR", "COP", "CLP", "MXN", "BRL", "PEN"]
        self.payment_methods = [
            "CREDIT_CARD", "DEBIT_CARD", "BANK_TRANSFER", 
            "DIGITAL_WALLET", "CASH", "CRYPTOCURRENCY"
        ]
        self.payment_statuses = [
            "PENDING", "COMPLETED", "FAILED", "CANCELLED", 
            "REFUNDED", "PROCESSING", "EXPIRED"
        ]
        self.action_types = [
            "LOGIN", "LOGOUT", "PURCHASE", "VIEW", "CLICK", 
            "SEARCH", "DOWNLOAD", "SHARE", "UNKNOWN"
        ]
        self.device_types = [
            "DESKTOP", "MOBILE", "TABLET", "TV", "SMARTWATCH", 
            "UNKNOWN", "IOT_DEVICE"
        ]
        
        # Configuraciones de error para testing
        self.error_types = {
            'payments': [
                'invalid_payment_id', 'invalid_customer_id', 'negative_amount',
                'invalid_currency', 'future_date', 'invalid_merchant_id',
                'missing_required_field', 'invalid_status', 'invalid_reference'
            ],
            'events': [
                'invalid_id', 'invalid_user_id', 'spam_action', 
                'negative_duration', 'excessive_page_views', 'invalid_device',
                'missing_timestamp', 'invalid_geo_data'
            ]
        }
    
    def _generate_payment_id(self, valid: bool = True) -> str:
        """Genera payment_id con formato PAY-XXXXXXXX-XXXX"""
        if not valid:
            # Generar ID inválido para testing
            return f"INVALID-{random.randint(1000, 9999)}"
        
        part1 = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))
        part2 = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=4))
        return f"PAY-{part1}-{part2}"
    
    def _generate_customer_id(self, valid: bool = True) -> str:
        """Genera customer_id con formato CUST-XXXXXX"""
        if not valid:
            return f"INV-{random.randint(100, 999)}"
        
        number = random.randint(100000, 999999)
        return f"CUST-{number:06d}"
    
    def _generate_reference_number(self, valid: bool = True) -> str:
        """Genera reference_number con formato REF-XXXXXXXXXXXX"""
        if not valid:
            return f"BADREF-{random.randint(1000, 9999)}"
        
        ref = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=12))
        return f"REF-{ref}"
    
    def _generate_record_id(self, valid: bool = True) -> str:
        """Genera UUID en formato específico"""
        if not valid:
            return f"INVALID-{random.randint(1000, 9999)}"
        
        return str(uuid.uuid4()).upper()
    
    def _generate_realistic_amount(self) -> float:
        """Genera montos con distribución realista"""
        # Distribución realista de montos de pago
        distribution = random.random()
        
        if distribution < 0.6:  # 60% pagos pequeños (1-100)
            return round(random.uniform(1, 100), 2)
        elif distribution < 0.85:  # 25% pagos medianos (100-1000)
            return round(random.uniform(100, 1000), 2)
        elif distribution < 0.95:  # 10% pagos grandes (1000-10000)
            return round(random.uniform(1000, 10000), 2)
        else:  # 5% pagos muy grandes (10000-100000)
            return round(random.uniform(10000, 100000), 2)
    
    def _generate_realistic_session_duration(self) -> int:
        """Genera duración de sesión realista"""
        distribution = random.random()
        
        if distribution < 0.1:  # 10% sesiones muy cortas (bounce)
            return 0
        elif distribution < 0.4:  # 30% sesiones cortas (1-5 min)
            return random.randint(1, 300)
        elif distribution < 0.7:  # 30% sesiones medianas (5-30 min)
            return random.randint(300, 1800)
        elif distribution < 0.9:  # 20% sesiones largas (30-120 min)
            return random.randint(1800, 7200)
        else:  # 10% sesiones muy largas (2+ horas)
            return random.randint(7200, 14400)
    
    def _introduce_payment_error(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Introduce errores específicos en registros de pagos para testing"""
        error_type = random.choice(self.error_types['payments'])
        
        if error_type == 'invalid_payment_id':
            record["Payment ID"] = self._generate_payment_id(valid=False)
        elif error_type == 'invalid_customer_id':
            record["Customer ID"] = self._generate_customer_id(valid=False)
        elif error_type == 'negative_amount':
            record["Payment Amount"] = -abs(float(record["Payment Amount"]))
        elif error_type == 'invalid_currency':
            record["Payment Currency"] = "INVALID_CURR"
        elif error_type == 'future_date':
            future_date = datetime.now() + timedelta(days=random.randint(1, 365))
            record["Payment Date"] = future_date.strftime("%Y-%m-%d %H:%M:%S")
        elif error_type == 'invalid_merchant_id':
            record["Merchant ID"] = random.randint(1000000, 9999999)  # Fuera de rango
        elif error_type == 'missing_required_field':
            # Eliminar campo requerido
            field_to_remove = random.choice(["Payment ID", "Customer ID", "Payment Amount"])
            record[field_to_remove] = ""
        elif error_type == 'invalid_status':
            record["Transaction Status"] = "INVALID_STATUS"
        elif error_type == 'invalid_reference':
            record["Reference Number"] = self._generate_reference_number(valid=False)
        
        return record
    
    def _introduce_event_error(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Introduce errores específicos en registros de eventos para testing"""
        error_type = random.choice(self.error_types['events'])
        
        if error_type == 'invalid_id':
            record["id"] = self._generate_record_id(valid=False)
        elif error_type == 'invalid_user_id':
            record["user_id"] = self._generate_customer_id(valid=False)
        elif error_type == 'spam_action':
            record["event_type"] = "SPAM"
        elif error_type == 'negative_duration':
            record["session_duration"] = -random.randint(1, 1000)
        elif error_type == 'excessive_page_views':
            record["page_views"] = random.randint(1001, 5000)
        elif error_type == 'invalid_device':
            record["device_type"] = "INVALID_DEVICE"
        elif error_type == 'missing_timestamp':
            record["timestamp"] = ""
        elif error_type == 'invalid_geo_data':
            if record.get("geo_location"):
                record["geo_location"]["latitude"] = 999.999  # Latitud inválida
        
        return record
    
    def generate_high_volume_payments(
        self, 
        num_records: int = 1000000, 
        batch_size: int = 50000,
        error_rate: float = 0.05
    ) -> str:
        """
        Genera dataset de pagos de alto volumen con características realistas
        
        Args:
            num_records: Número total de registros a generar
            batch_size: Tamaño de lote para procesamiento en memoria
            error_rate: Porcentaje de registros con errores (0.0-1.0)
        
        Returns:
            str: Ruta del directorio de salida
        """
        logger.info(f"[INICIO] Iniciando generación de {num_records:,} registros de pagos")
        logger.info(f"[CONFIG] Configuración: batch_size={batch_size:,}, error_rate={error_rate:.1%}")
        
        # Crear directorio de salida
        output_dir = self.output_base_dir / "payments-high-volume"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Calcular número de lotes
        num_batches = (num_records + batch_size - 1) // batch_size
        
        for batch_num in range(num_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, num_records)
            batch_records = end_idx - start_idx
            
            logger.info(f"[LOTE] Generando lote {batch_num + 1}/{num_batches} ({batch_records:,} registros)")
            
            batch_data = []
            for i in range(batch_records):
                # Generar fechas realistas (últimos 2 años con distribución no uniforme)
                days_ago = random.choices(
                    range(0, 730),  # 2 años
                    weights=[3 if d < 30 else 2 if d < 90 else 1 for d in range(0, 730)]  # Más recientes más probables
                )[0]
                
                payment_date = datetime.now() - timedelta(days=days_ago)
                payment_date = payment_date.replace(
                    hour=random.randint(0, 23),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )
                
                # Updated_at siempre después de payment_date
                updated_at = payment_date + timedelta(
                    seconds=random.randint(0, 86400)  # Hasta 24 horas después
                )
                
                # Generar monto realista
                amount = self._generate_realistic_amount()
                
                # Crear registro base
                record = {
                    "Payment ID": self._generate_payment_id(),
                    "Customer ID": self._generate_customer_id(),
                    "Payment Amount": amount,
                    "Payment Currency": random.choice(self.currencies),
                    "Payment Date": payment_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "Last Updated": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "Payment Method": random.choice(self.payment_methods),
                    "Transaction Status": random.choice(self.payment_statuses),
                    "Merchant ID": random.randint(1, 999999),
                    "Reference Number": self._generate_reference_number() if random.random() > 0.3 else ""
                }
                
                # Introducir errores según la tasa configurada
                has_error = random.random() < error_rate
                if has_error:
                    record = self._introduce_payment_error(record)
                
                batch_data.append(record)
                self.metrics.record_generated(with_error=has_error)
            
            # Escribir lote a archivo CSV
            filename = output_dir / f"payments_batch_{batch_num + 1:03d}.csv"
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                if batch_data:
                    fieldnames = batch_data[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(batch_data)
            
            self.metrics.file_created(str(filename))
            logger.info(f"[SAVE] Lote guardado: {filename} ({os.path.getsize(filename) / 1024 / 1024:.1f} MB)")
        
        # Métricas finales
        summary = self.metrics.get_summary()
        logger.info(f"[METRICS] Generación completa - Métricas:")
        logger.info(f"   [RECORDS] Registros: {summary['records_generated']:,}")
        logger.info(f"   [FILES] Archivos: {summary['files_created']}")
        logger.info(f"   [ERRORS] Errores: {summary['errors_introduced']:,} ({summary['error_rate_percent']:.1f}%)")
        logger.info(f"   [SIZE] Tamaño: {summary['total_size_mb']:.1f} MB")
        logger.info(f"   [SPEED] Velocidad: {summary['records_per_second']:.0f} rec/sec")
        logger.info(f"   [TIME] Tiempo: {summary['elapsed_time_seconds']:.1f} segundos")
        
        return str(output_dir)
    
    def generate_multi_format_events(
        self, 
        num_records: int = 500000, 
        batch_size: int = 25000,
        error_rate: float = 0.03
    ) -> str:
        """
        Genera dataset multi-formato con eventos complejos
        
        Args:
            num_records: Número total de registros a generar
            batch_size: Tamaño de lote para procesamiento en memoria
            error_rate: Porcentaje de registros con errores (0.0-1.0)
        
        Returns:
            str: Ruta del directorio de salida
        """
        logger.info(f"[START] Iniciando generación de {num_records:,} eventos multi-formato")
        logger.info(f"[CONFIG] Configuración: batch_size={batch_size:,}, error_rate={error_rate:.1%}")
        
        # Crear directorio de salida
        output_dir = self.output_base_dir / "multi-format"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Calcular número de lotes
        num_batches = (num_records + batch_size - 1) // batch_size
        
        for batch_num in range(num_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, num_records)
            batch_records = end_idx - start_idx
            
            logger.info(f"[BATCH] Generando lote JSON {batch_num + 1}/{num_batches} ({batch_records:,} registros)")
            
            batch_data = []
            for i in range(batch_records):
                # Generar timestamp realista (último año)
                days_ago = random.randint(0, 365)
                event_timestamp = datetime.now() - timedelta(days=days_ago)
                event_timestamp = event_timestamp.replace(
                    hour=random.randint(0, 23),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59),
                    microsecond=random.randint(0, 999999)
                )
                
                # Generar duración de sesión realista
                session_duration = self._generate_realistic_session_duration()
                
                # Page views correlacionado con duración
                if session_duration == 0:
                    page_views = 1
                elif session_duration < 300:
                    page_views = random.randint(1, 5)
                elif session_duration < 1800:
                    page_views = random.randint(3, 15)
                else:
                    page_views = random.randint(10, 50)
                
                # Tipo de acción y valor de conversión
                action_type = random.choice(self.action_types)
                conversion_value = None
                if action_type == "PURCHASE":
                    conversion_value = round(random.uniform(10, 5000), 4)
                elif action_type in ["VIEW", "CLICK"] and random.random() < 0.1:
                    conversion_value = round(random.uniform(1, 100), 4)  # Micro-conversiones
                
                # Crear registro base
                record = {
                    "id": self._generate_record_id(),
                    "user_id": self._generate_customer_id(),
                    "timestamp": event_timestamp.isoformat() + "Z",
                    "event_type": action_type,
                    "session_duration": session_duration,
                    "page_views": page_views,
                    "conversion_value": conversion_value,
                    "device_type": random.choice(self.device_types),
                    "metadata": {
                        "user_agent": fake.user_agent(),
                        "ip_address": fake.ipv4(),
                        "session_id": str(uuid.uuid4()),
                        "referrer": fake.url() if random.random() > 0.3 else None,
                        "campaign_id": f"camp_{random.randint(1000, 9999)}" if random.random() > 0.7 else None
                    }
                }
                
                # Agregar geo_location condicionalmente
                if random.random() > 0.2:  # 80% de registros tienen geo_location
                    record["geo_location"] = {
                        "country": fake.country_code(),
                        "city": fake.city(),
                        "latitude": float(fake.latitude()),
                        "longitude": float(fake.longitude())
                    }
                
                # Introducir errores según la tasa configurada
                has_error = random.random() < error_rate
                if has_error:
                    record = self._introduce_event_error(record)
                
                batch_data.append(record)
                self.metrics.record_generated(with_error=has_error)
            
            # Escribir lote a archivo JSON (JSONL format)
            filename = output_dir / f"events_batch_{batch_num + 1:03d}.json"
            with open(filename, 'w', encoding='utf-8') as jsonfile:
                for record in batch_data:
                    json.dump(record, jsonfile, ensure_ascii=False, default=str)
                    jsonfile.write('\n')
            
            self.metrics.file_created(str(filename))
            logger.info(f"[SAVE] Lote JSON guardado: {filename} ({os.path.getsize(filename) / 1024 / 1024:.1f} MB)")
        
        # Métricas finales
        summary = self.metrics.get_summary()
        logger.info(f"[METRICS] Generación multi-formato completa - Métricas:")
        logger.info(f"   [RECORDS] Registros: {summary['records_generated']:,}")
        logger.info(f"   [FILES] Archivos: {summary['files_created']}")
        logger.info(f"   [ERRORS] Errores: {summary['errors_introduced']:,} ({summary['error_rate_percent']:.1f}%)")
        logger.info(f"   [SIZE] Tamaño: {summary['total_size_mb']:.1f} MB")
        logger.info(f"   [SPEED] Velocidad: {summary['records_per_second']:.0f} rec/sec")
        logger.info(f"   [TIME] Tiempo: {summary['elapsed_time_seconds']:.1f} segundos")
        
        return str(output_dir)
    
    def generate_configuration_files(self, case_type: str, output_dir: str):
        """Genera archivos de configuración para los casos de uso"""
        config_dir = Path("config/datasets/casos_uso")
        config_dir.mkdir(parents=True, exist_ok=True)
        
        if case_type == "payments":
            # Generar configuraciones para pagos de alto volumen
            self._generate_payments_config(config_dir)
        elif case_type == "multiformat":
            # Generar configuraciones para multi-formato
            self._generate_multiformat_config(config_dir)
    
    def _generate_payments_config(self, config_dir: Path):
        """Genera archivos de configuración para el caso de uso de pagos"""
        # Dataset configuration
        dataset_config = {
            "id": "payments_high_volume",
            "source": {
                "input_format": "csv",
                "path": "s3a://raw/casos-uso/payments-high-volume/*.csv",
                "options": {
                    "header": "true",
                    "inferSchema": "false",
                    "multiline": "true",
                    "escape": "\"",
                    "quote": "\""
                }
            },
            "standardization": {
                "timezone": "America/Bogota",
                "rename": [
                    {"from": "Payment ID", "to": "payment_id"},
                    {"from": "Customer ID", "to": "customer_id"},
                    {"from": "Payment Amount", "to": "amount"},
                    {"from": "Payment Currency", "to": "currency"},
                    {"from": "Payment Date", "to": "payment_date"},
                    {"from": "Last Updated", "to": "updated_at"},
                    {"from": "Payment Method", "to": "payment_method"},
                    {"from": "Transaction Status", "to": "status"},
                    {"from": "Merchant ID", "to": "merchant_id"},
                    {"from": "Reference Number", "to": "reference_number"}
                ],
                "casts": [
                    {"column": "amount", "to": "decimal(18,2)", "on_error": "null"},
                    {"column": "payment_date", "to": "timestamp", "format_hint": "yyyy-MM-dd HH:mm:ss", "on_error": "null"},
                    {"column": "updated_at", "to": "timestamp", "format_hint": "yyyy-MM-dd HH:mm:ss", "on_error": "null"},
                    {"column": "merchant_id", "to": "integer", "on_error": "null"}
                ],
                "defaults": [
                    {"column": "currency", "value": "USD"},
                    {"column": "status", "value": "PENDING"}
                ],
                "deduplicate": {
                    "key": ["payment_id", "customer_id"],
                    "order_by": ["updated_at desc", "amount desc"]
                }
            },
            "quality": {
                "expectations_ref": "config/datasets/casos_uso/payments_high_volume_expectations.yml",
                "quarantine": "s3a://raw/quarantine/payments-high-volume/"
            },
            "schema": {
                "ref": "config/datasets/casos_uso/payments_high_volume_schema.json",
                "mode": "strict"
            },
            "output": {
                "silver": {
                    "format": "parquet",
                    "path": "s3a://silver/payments-high-volume/",
                    "partition_by": ["year", "month"],
                    "merge_schema": True,
                    "mode": "overwrite_dynamic",
                    "partition_from": "payment_date"
                },
                "gold": {
                    "enabled": True,
                    "database_config": "config/database.yml",
                    "environment": "development",
                    "exclude_columns": ["_run_id", "_ingestion_ts", "year", "month"],
                    "add_columns": [
                        {"name": "data_source", "value": "high_volume_payments", "type": "string"},
                        {"name": "processed_at", "value": "current_timestamp()", "type": "timestamp"},
                        {"name": "batch_id", "value": "uuid()", "type": "string"}
                    ],
                    "business_rules": [
                        {"condition": "amount > 0", "action": "filter"},
                        {"condition": "currency IS NOT NULL", "action": "filter"},
                        {"condition": "status IN ('COMPLETED', 'PENDING', 'FAILED')", "action": "filter"},
                        {"condition": "payment_date >= '2020-01-01'", "action": "filter"}
                    ]
                }
            }
        }
        
        # Escribir configuración del dataset
        with open(config_dir / "payments_high_volume.yml", 'w') as f:
            import yaml
            yaml.dump(dataset_config, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"[CONFIG] Configuración de pagos generada en: {config_dir / 'payments_high_volume.yml'}")
    
    def _generate_multiformat_config(self, config_dir: Path):
        """Genera archivos de configuración para el caso de uso multi-formato"""
        # Dataset configuration
        dataset_config = {
            "id": "events_multiformat_extreme",
            "source": {
                "input_format": "json",
                "path": "s3a://raw/casos-uso/events-multiformat/*.json",
                "options": {
                    "multiline": "true",
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record"
                }
            },
            "schema": {
                "mode": "strict",
                "ref": "config/datasets/casos_uso/events_multiformat_schema.json"
            },
            "quality": {
                "expectations_ref": "config/datasets/casos_uso/events_multiformat_expectations.yml",
                "quarantine": "s3a://raw/quarantine/events-multiformat/"
            },
            "output": {
                "silver": {
                    "path": "s3a://silver/events-multiformat/",
                    "format": "parquet",
                    "mode": "overwrite_dynamic",
                    "partition_by": ["year", "month", "event_type"],
                    "partition_from": "event_timestamp",
                    "merge_schema": True
                },
                "gold": {
                    "enabled": True,
                    "environment": "development",
                    "database_config": "config/database.yml",
                    "business_rules": [
                        {"action": "filter", "condition": "event_id IS NOT NULL"},
                        {"action": "filter", "condition": "user_id IS NOT NULL"},
                        {"action": "filter", "condition": "event_timestamp IS NOT NULL"},
                        {"action": "filter", "condition": "event_type IN ('PAGE_VIEW', 'CLICK', 'PURCHASE', 'LOGIN', 'LOGOUT', 'SEARCH', 'ERROR')"}
                    ],
                    "add_columns": [
                        {"name": "data_source", "type": "string", "value": "multiformat_events"},
                        {"name": "processed_at", "type": "timestamp", "value": "current_timestamp()"},
                        {"name": "batch_id", "type": "string", "value": "uuid()"}
                    ],
                    "exclude_columns": ["_run_id", "_ingestion_ts", "year", "month", "_corrupt_record"]
                }
            }
        }
        
        # Escribir configuración del dataset
        with open(config_dir / "events_multiformat_extreme.yml", 'w') as f:
            import yaml
            yaml.dump(dataset_config, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"[CONFIG] Configuración multi-formato generada en: {config_dir / 'events_multiformat_extreme.yml'}")

def main():
    """Función principal con interfaz de línea de comandos"""
    parser = argparse.ArgumentParser(
        description="Generador de datos sintéticos para casos de uso avanzados",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python scripts/generate_synthetic_data.py --case payments --records 1000000
  python scripts/generate_synthetic_data.py --case multiformat --records 500000
  python scripts/generate_synthetic_data.py --case all
  python scripts/generate_synthetic_data.py --case payments --records 100000 --error-rate 0.1
        """
    )
    
    parser.add_argument(
        '--case', 
        choices=['payments', 'multiformat', 'all'],
        default='all',
        help='Tipo de caso de uso a generar (default: all)'
    )
    
    parser.add_argument(
        '--records',
        type=int,
        help='Número de registros a generar (default: 1M para payments, 500K para multiformat)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=50000,
        help='Tamaño de lote para procesamiento (default: 50000)'
    )
    
    parser.add_argument(
        '--error-rate',
        type=float,
        default=0.05,
        help='Tasa de errores a introducir (0.0-1.0, default: 0.05)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/casos-uso',
        help='Directorio base de salida (default: data/casos-uso)'
    )
    
    parser.add_argument(
        '--config-only',
        action='store_true',
        help='Solo generar archivos de configuración, no datos'
    )
    
    args = parser.parse_args()
    
    # Validar argumentos
    if args.error_rate < 0 or args.error_rate > 1:
        logger.error("Error rate debe estar entre 0.0 y 1.0")
        sys.exit(1)
    
    # Crear generador
    generator = AdvancedSyntheticDataGenerator(args.output_dir)
    
    logger.info("[INICIO] GENERADOR DE DATOS SINTÉTICOS - CASOS DE USO AVANZADOS")
    logger.info("=" * 60)
    
    try:
        if args.case in ['payments', 'all']:
            records = args.records or 1000000
            logger.info(f"\n[CASO 1] Pagos de Alto Volumen ({records:,} registros)")
            
            if not args.config_only:
                payments_dir = generator.generate_high_volume_payments(
                    num_records=records,
                    batch_size=args.batch_size,
                    error_rate=args.error_rate
                )
                logger.info(f"[OK] Datos de pagos generados en: {payments_dir}")
            
            generator.generate_configuration_files('payments', args.output_dir)
        
        if args.case in ['multiformat', 'all']:
            records = args.records or 500000
            logger.info(f"\n[CASO 2] Multi-formato Extremo ({records:,} registros)")
            
            if not args.config_only:
                multiformat_dir = generator.generate_multi_format_events(
                    num_records=records,
                    batch_size=args.batch_size,
                    error_rate=args.error_rate
                )
                logger.info(f"[OK] Datos multi-formato generados en: {multiformat_dir}")
            
            generator.generate_configuration_files('multiformat', args.output_dir)
        
        # Resumen final
        final_summary = generator.metrics.get_summary()
        logger.info("\n" + "=" * 60)
        logger.info("[COMPLETO] GENERACIÓN COMPLETA - RESUMEN FINAL")
        logger.info(f"[STATS] Total de registros: {final_summary['records_generated']:,}")
        logger.info(f"[STATS] Total de archivos: {final_summary['files_created']}")
        logger.info(f"[STATS] Tamaño total: {final_summary['total_size_mb']:.1f} MB")
        logger.info(f"[STATS] Velocidad promedio: {final_summary['records_per_second']:.0f} rec/sec")
        logger.info(f"[STATS] Tiempo total: {final_summary['elapsed_time_seconds']:.1f} segundos")
        
        logger.info("\n[NEXT] PRÓXIMOS PASOS:")
        if generator.is_s3_target:
            logger.info("1. Copiar archivos a MinIO/S3:")
            logger.info(f"   mc cp --recursive {generator.output_base_dir}/ local/{generator.s3_target_path.replace('s3a://', '')}")
        else:
            logger.info("1. Copiar archivos a MinIO/S3:")
            logger.info("   mc cp --recursive data/casos-uso/ local/raw/casos-uso/")
        logger.info("2. Ejecutar pipelines:")
        logger.info("   python pipelines/spark_job_with_db.py config/datasets/casos_uso/payments_high_volume.yml ...")
        logger.info("3. Validar resultados y métricas de rendimiento")
        
    except KeyboardInterrupt:
        logger.warning("\n[WARN] Generación interrumpida por el usuario")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n[ERROR] Error durante la generación: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()