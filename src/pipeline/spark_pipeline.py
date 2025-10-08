"""
Pipeline de datos refactorizado con separación completa de lógica y configuración.
Soporta tanto desarrollo local como despliegue en Azure.
"""
import sys
import os
import yaml
import re
import json
import pytz
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType

try:
    from ..config.settings import config_manager
    from ..security.keyvault import secret_manager
    from ..utils.logging_config import setup_logging
    from ..utils.data_quality import DataQualityValidator
    from ..utils.schema_utils import SchemaManager
except ImportError:
    try:
        from config.settings import config_manager
        from security.keyvault import secret_manager
        from utils.logging_config import setup_logging
        from utils.data_quality import DataQualityValidator
        from utils.schema_utils import SchemaManager
    except ImportError:
        from src.config.settings import config_manager
        from src.security.keyvault import secret_manager
        from src.utils.logging_config import setup_logging
        from src.utils.data_quality import DataQualityValidator
        from src.utils.schema_utils import SchemaManager

logger = logging.getLogger(__name__)

class SparkPipelineProcessor:
    """Procesador principal del pipeline de datos"""
    
    def __init__(self, config_path: str, env_path: str):
        self.config_path = config_path
        self.env_path = env_path
        self.config = self._load_dataset_config()
        self.env_config = self._load_env_config()
        self.spark = self._create_spark_session()
        self.schema_manager = SchemaManager()
        self.quality_validator = DataQualityValidator()
        
        # Configurar credenciales de almacenamiento (solo si no estamos en modo de prueba)
        if not os.getenv('TEST_MODE', 'false').lower() == 'true':
            secret_manager.set_spark_configs(self.spark)
        
    def _load_dataset_config(self) -> Dict[str, Any]:
        """Carga configuración del dataset"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading dataset config: {e}")
            raise
    
    def _load_env_config(self) -> Dict[str, Any]:
        """Carga configuración del entorno"""
        try:
            with open(self.env_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading environment config: {e}")
            raise
    
    def _create_spark_session(self) -> SparkSession:
        """Crea sesión de Spark con configuración apropiada"""
        app_name = f"cfg-pipeline::{self.config['id']}"
        timezone = config_manager.get_timezone()
        
        builder = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone", timezone)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        )
        
        # Configuraciones específicas para Azure
        if config_manager.environment != 'local':
            builder = builder.config("spark.hadoop.fs.azure.secure.mode", "true")
        
        return builder.getOrCreate()
    
    def _resolve_path_template(self, template: str, **kwargs) -> str:
        """Resuelve plantillas de path con variables"""
        # Obtener fecha de ejecución
        tz = config_manager.get_timezone()
        today = datetime.now(pytz.timezone(tz))
        run_date = os.getenv('RUN_DATE', today.strftime(
            self.env_config.get('date_pattern', '%Y/%m/%d')
        ))
        
        # Resolver template
        resolved = template.format(
            fs=config_manager.get_fs_path(),
            run_date=run_date,
            **kwargs
        )
        
        logger.info(f"Resolved path: {template} -> {resolved}")
        return resolved
    
    def _read_source_data(self) -> DataFrame:
        """Lee datos de la fuente configurada"""
        src_config = self.config['source']
        src_path = self._resolve_path_template(src_config['path'])
        
        logger.info(f"Reading from: {src_path}")
        
        reader = self.spark.read.options(**src_config.get('options', {}))
        input_format = src_config['input_format']
        
        if input_format == 'csv':
            df = reader.csv(src_path)
        elif input_format in ('json', 'jsonl'):
            df = reader.json(src_path)
        elif input_format == 'parquet':
            df = reader.parquet(src_path)
        elif input_format == 'delta':
            df = reader.format('delta').load(src_path)
        else:
            raise ValueError(f"Formato no soportado: {input_format}")
        
        logger.info(f"Read {df.count()} rows from source")
        return df
    
    def _apply_standardization(self, df: DataFrame) -> DataFrame:
        """Aplica transformaciones de estandarización"""
        std_config = self.config.get('standardization', {})
        
        # Renombrar columnas
        for rename_rule in std_config.get('rename', []):
            if rename_rule['from'] in df.columns:
                df = df.withColumnRenamed(rename_rule['from'], rename_rule['to'])
                logger.debug(f"Renamed column: {rename_rule['from']} -> {rename_rule['to']}")
        
        # Aplicar esquema si está definido
        if 'schema' in self.config and self.config['schema'].get('ref'):
            schema_path = self.config['schema']['ref']
            mode = self.config['schema'].get('mode', 'strict')
            df = self.schema_manager.enforce_schema(df, schema_path, mode)
        
        # Conversiones de tipo
        for cast_rule in std_config.get('casts', []):
            df = self._safe_cast(
                df, 
                cast_rule['column'], 
                cast_rule['to'],
                fmt=cast_rule.get('format_hint'),
                on_error=cast_rule.get('on_error', 'fail')
            )
        
        # Valores por defecto
        for default_rule in std_config.get('defaults', []):
            col, val = default_rule['column'], default_rule['value']
            df = df.withColumn(
                col, 
                F.when(F.col(col).isNull(), F.lit(val)).otherwise(F.col(col))
            )
        
        # Deduplicación
        if 'deduplicate' in std_config:
            df = self._deduplicate(df, std_config['deduplicate'])
        
        return df
    
    def _safe_cast(self, df: DataFrame, col: str, target: str, 
                   fmt: Optional[str] = None, on_error: str = "fail") -> DataFrame:
        """Conversión segura de tipos"""
        target_type = self.schema_manager.normalize_type(target)
        c = F.col(col)
        
        if target_type == "timestamp":
            new_col = F.to_timestamp(c, fmt) if fmt else F.to_timestamp(c)
        elif target_type == "date":
            new_col = F.to_date(c, fmt) if fmt else F.to_date(c)
        else:
            if on_error == "null":
                new_col = F.expr(f"try_cast({col} as {target_type})")
            else:
                new_col = c.cast(target_type)
        
        return df.withColumn(col, new_col)
    
    def _deduplicate(self, df: DataFrame, dedup_config: Dict[str, Any]) -> DataFrame:
        """Aplica deduplicación basada en configuración"""
        key_cols = dedup_config['key']
        order_exprs = self._parse_order_expressions(dedup_config.get('order_by', []))
        
        if order_exprs:
            window = Window.partitionBy(*key_cols).orderBy(*order_exprs)
        else:
            window = Window.partitionBy(*key_cols)
        
        df_dedup = (df
                   .withColumn("_rn", F.row_number().over(window))
                   .filter(F.col("_rn") == 1)
                   .drop("_rn"))
        
        logger.info(f"Deduplicated on keys: {key_cols}")
        return df_dedup
    
    def _parse_order_expressions(self, order_exprs: List[str]) -> List:
        """Parsea expresiones de ordenamiento"""
        parsed = []
        for expr in order_exprs:
            parts = expr.strip().split()
            col_name = parts[0]
            is_desc = len(parts) > 1 and parts[1].lower() == "desc"
            parsed.append(F.col(col_name).desc() if is_desc else F.col(col_name).asc())
        return parsed
    
    def _add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """Añade columnas de metadata"""
        run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
        
        df = df.withColumn('_run_id', F.lit(run_id))
        df = df.withColumn('_ingestion_ts', F.current_timestamp())
        
        # Añadir particiones de fecha si existe columna payment_date
        if 'payment_date' in df.columns:
            df = (df
                 .withColumn('year', F.year('payment_date'))
                 .withColumn('month', F.month('payment_date')))
        
        return df
    
    def _apply_quality_checks(self, df: DataFrame) -> DataFrame:
        """Aplica validaciones de calidad de datos"""
        if 'quality' not in self.config:
            return df
        
        quality_config = self.config['quality']
        expectations_path = quality_config.get('expectations_ref')
        quarantine_path = quality_config.get('quarantine')
        
        if expectations_path:
            quarantine_resolved = self._resolve_path_template(quarantine_path) if quarantine_path else None
            df_clean, df_quarantine, stats = self.quality_validator.validate(
                df, expectations_path, quarantine_resolved
            )
            
            logger.info(f"Quality check stats: {stats}")
            return df_clean
        
        return df
    
    def _write_output(self, df: DataFrame) -> None:
        """Escribe datos al destino configurado"""
        output_config = self.config['output']['silver']
        output_path = self._resolve_path_template(output_config['path'])
        
        logger.info(f"Writing to: {output_path}")
        
        writer = (df.write
                 .format(output_config.get('format', 'parquet'))
                 .option('mergeSchema', str(output_config.get('merge_schema', True)).lower()))
        
        # Configurar particionado
        partition_cols = output_config.get('partition_by', [])
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        # Configurar modo de escritura
        write_mode = output_config.get('mode', 'append').lower()
        if write_mode == 'overwrite_dynamic':
            writer = writer.mode('overwrite')
        else:
            writer = writer.mode(write_mode)
        
        writer.save(output_path)
        logger.info(f"Successfully wrote data to {output_path}")
    
    def run(self) -> None:
        """Ejecuta el pipeline completo"""
        try:
            logger.info(f"Starting pipeline: {self.config['id']}")
            
            # 1. Leer datos de origen
            df = self._read_source_data()
            
            # 2. Aplicar estandarización
            df = self._apply_standardization(df)
            
            # 3. Añadir metadata
            df = self._add_metadata_columns(df)
            
            # 4. Validaciones de calidad
            df = self._apply_quality_checks(df)
            
            # 5. Escribir resultado
            self._write_output(df)
            
            logger.info(f"Pipeline completed successfully: {self.config['id']}")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            self.spark.stop()

def main():
    """Función principal del pipeline"""
    if len(sys.argv) != 3:
        print("Usage: python spark_pipeline.py <config_path> <env_path>")
        sys.exit(1)
    
    config_path, env_path = sys.argv[1], sys.argv[2]
    
    # Configurar logging
    setup_logging()
    
    # Ejecutar pipeline
    processor = SparkPipelineProcessor(config_path, env_path)
    processor.run()

if __name__ == "__main__":
    main()